package grok

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// Handler is the top-level Grok WebSocket handler.
type Handler struct {
	config          *Config
	upgrader        websocket.Upgrader
	sessionManager  *SessionManager
	captchaVerifier *CaptchaVerifier
	xaiClient       *XAIClient
}

// NewHandler creates a new Grok handler. apiKey and captchaSecret are loaded by
// the caller (env or otherwise) — we do not read .env ourselves.
func NewHandler(cfg *Config, apiKey string, captchaSecret string) *Handler {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  cfg.WebSocket.ReadBufferSize,
		WriteBufferSize: cfg.WebSocket.WriteBufferSize,
		CheckOrigin:     makeOriginChecker(cfg.WebSocket.AllowedOrigins),
	}

	return &Handler{
		config:          cfg,
		upgrader:        upgrader,
		sessionManager:  NewSessionManager(cfg),
		captchaVerifier: NewCaptchaVerifier(captchaSecret, cfg.Captcha.VerifyURL),
		xaiClient:       NewXAIClient(apiKey, cfg),
	}
}

// ServeHTTP implements http.Handler — upgrades to WebSocket and runs the session.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sess, errMsg := h.sessionManager.CreateSession(r)
	if sess == nil {
		http.Error(w, errMsg, http.StatusTooManyRequests)
		return
	}
	defer h.sessionManager.DestroySession(sess)

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logrus.WithError(err).Warn("grok ws upgrade failed")
		return
	}
	defer conn.Close()

	conn.SetReadLimit(h.config.WebSocket.MaxMessageSize)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var writeMu sync.Mutex

	// Configure keepalive
	conn.SetReadDeadline(time.Now().Add(h.config.PongWait()))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(h.config.PongWait()))
		return nil
	})

	// Ping goroutine
	go h.pingLoop(ctx, conn, &writeMu)

	// Idle timeout goroutine
	idleTimer := time.NewTimer(h.config.IdleTimeout())
	defer idleTimer.Stop()
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-idleTimer.C:
			logrus.WithField("session", sess.ID).Info("session idle timeout")
			writeMu.Lock()
			sendMsg(conn, &writeMu, ServerMessage{Type: "error", Content: "session timed out due to inactivity"})
			writeMu.Unlock()
			conn.Close()
		}
	}()

	// If captcha is enabled, require it before any messages
	if sess.NeedsCaptcha(h.config) {
		sendMsg(conn, &writeMu, ServerMessage{
			Type: "captcha_required",
		})
	} else {
		sendMsg(conn, &writeMu, ServerMessage{
			Type:    "info",
			Content: "session ready",
		})
	}

	// Main message loop
	for {
		var msg ClientMessage
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logrus.WithError(err).Debug("grok ws read error")
			}
			return
		}

		// Reset idle timer on any message
		idleTimer.Reset(h.config.IdleTimeout())

		switch msg.Type {
		case "captcha":
			h.handleCaptcha(conn, &writeMu, sess, msg)

		case "message":
			h.handleMessage(ctx, conn, &writeMu, sess, msg)

		case "tool_results":
			h.handleToolResults(ctx, conn, &writeMu, sess, msg)

		case "clear":
			sess.ClearHistory(h.config)
			sendMsg(conn, &writeMu, ServerMessage{Type: "info", Content: "conversation cleared"})

		default:
			sendMsg(conn, &writeMu, ServerMessage{
				Type:    "error",
				Content: "unknown message type: " + msg.Type,
			})
		}
	}
}

func (h *Handler) handleCaptcha(conn *websocket.Conn, writeMu *sync.Mutex, sess *Session, msg ClientMessage) {
	if !h.config.Captcha.Enabled {
		sendMsg(conn, writeMu, ServerMessage{Type: "captcha_ok", Content: "captcha disabled"})
		return
	}

	if msg.Token == "" {
		sendMsg(conn, writeMu, ServerMessage{Type: "error", Content: "captcha token required"})
		return
	}

	if err := h.captchaVerifier.Verify(msg.Token, sess.IP); err != nil {
		logrus.WithError(err).WithField("session", sess.ID).Warn("captcha failed")
		sendMsg(conn, writeMu, ServerMessage{Type: "error", Content: "captcha verification failed"})
		return
	}

	sess.MarkCaptchaVerified()

	// Tell the client captcha is OK, but also include whether we have a
	// pending message that will be replayed automatically.
	hasPending := sess.PendingMessage != ""
	sendMsg(conn, writeMu, ServerMessage{Type: "captcha_ok", Content: "verified"})
	logrus.WithField("session", sess.ID).Info("captcha verified")

	// Replay the message that was blocked by captcha, if any.
	if hasPending {
		pending := sess.PendingMessage
		sess.PendingMessage = ""
		logrus.WithField("session", sess.ID).Info("replaying pending message after captcha")
		sess.AddUserMessage(pending, h.config)
		// Run in the request context; gorilla conn is already alive
		h.streamResponse(context.Background(), conn, writeMu, sess)
	}
}

func (h *Handler) handleMessage(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, sess *Session, msg ClientMessage) {
	// Check captcha — if needed, stash the message content so we can
	// replay it automatically once the user re-verifies.
	if sess.NeedsCaptcha(h.config) {
		if msg.Content != "" {
			sess.PendingMessage = msg.Content
		}
		sendMsg(conn, writeMu, ServerMessage{
			Type:    "captcha_required",
			Content: "captcha verification required before continuing",
		})
		return
	}

	// Check rate limit
	if !sess.CanSendMessage(h.config) {
		sendMsg(conn, writeMu, ServerMessage{
			Type:    "error",
			Content: "slow down — messages rate limited",
		})
		return
	}

	if msg.Content == "" {
		sendMsg(conn, writeMu, ServerMessage{Type: "error", Content: "empty message"})
		return
	}

	sess.AddUserMessage(msg.Content, h.config)
	h.streamResponse(ctx, conn, writeMu, sess)
}

func (h *Handler) handleToolResults(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, sess *Session, msg ClientMessage) {
	// Do NOT gate tool_results behind captcha — the original user message
	// that triggered these tool calls was already authenticated. Blocking
	// here would lose the tool results and break the conversation flow.

	for _, res := range msg.Results {
		sess.AddToolResult(res.ToolCallID, res.Name, res.Content, h.config)
	}

	h.streamResponse(ctx, conn, writeMu, sess)
}

func (h *Handler) streamResponse(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex, sess *Session) {
	cb := StreamCallback{
		OnPartial: func(text string) error {
			return sendMsg(conn, writeMu, ServerMessage{Type: "partial", Content: text})
		},
		OnToolCalls: func(calls []ToolCall) error {
			// Separate server-side tools from client tools
			var clientTools []ToolCall
			var serverTools []ToolCall

			for _, call := range calls {
				if call.Function.Name == "request_grok_research" {
					serverTools = append(serverTools, call)
				} else {
					clientTools = append(clientTools, call)
				}
			}

			// Add all tool calls to session history first
			sess.AddAssistantToolCalls(calls, h.config)

			// Handle server-side tools
			for _, serverTool := range serverTools {
				// Parse arguments to get the reason for the message
				var args struct {
					Query  string `json:"query"`
					Reason string `json:"reason"`
				}
				var description string
				if err := json.Unmarshal([]byte(serverTool.Function.Arguments), &args); err == nil && args.Reason != "" {
					description = args.Reason
				} else {
					description = "Performing deep research using xAI's agentic tools..."
				}

				// Send notification to client that tool is executing
				sendMsg(conn, writeMu, ServerMessage{
					Type:     "tool_executing",
					Content:  description,
					ToolName: serverTool.Function.Name,
					Calls:    []ToolCall{serverTool},
				})

				// Execute the tool
				result, err := h.executeServerTool(ctx, serverTool)
				if err != nil {
					logrus.WithError(err).WithField("tool", serverTool.Function.Name).Error("server tool execution failed")
					result = fmt.Sprintf("Research failed: %s", err.Error())
				}

				// Add tool result to session
				sess.AddToolResult(serverTool.ID, serverTool.Function.Name, result, h.config)

				// Send completion notification to client
				sendMsg(conn, writeMu, ServerMessage{
					Type:     "tool_complete",
					Content:  "Research complete",
					ToolName: serverTool.Function.Name,
				})
			}

			// Send client tools for execution (if any)
			if len(clientTools) > 0 {
				return sendMsg(conn, writeMu, ServerMessage{Type: "tool_calls", Calls: clientTools})
			}

			// If only server tools, continue streaming immediately
			if len(serverTools) > 0 && len(clientTools) == 0 {
				h.streamResponse(ctx, conn, writeMu, sess)
			}

			return nil
		},
		OnComplete: func(fullContent string) error {
			sess.AddAssistantMessage(fullContent, h.config)
			return sendMsg(conn, writeMu, ServerMessage{Type: "message", Content: fullContent})
		},
	}

	if err := h.xaiClient.StreamCompletion(ctx, sess.History, cb); err != nil {
		logrus.WithError(err).WithField("session", sess.ID).Error("xAI stream error")
		sendMsg(conn, writeMu, ServerMessage{Type: "error", Content: "AI request failed: " + err.Error()})
	}
}

func (h *Handler) executeServerTool(ctx context.Context, toolCall ToolCall) (string, error) {
	switch toolCall.Function.Name {
	case "request_grok_research":
		// Parse arguments
		var args struct {
			Query string `json:"query"`
		}

		if err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args); err != nil {
			logrus.WithError(err).Error("failed to parse tool arguments")
			return "", fmt.Errorf("parsing arguments: %w", err)
		}

		// Create research messages
		messages := []map[string]any{
			{
				"role":    "system",
				"content": "You are a concise research agent using real-time web/X/code tools. Answer factually, cite sources, be brief.",
			},
			{
				"role":    "user",
				"content": args.Query,
			},
		}

		// Execute agentic research
		logrus.WithField("query", args.Query).Info("executing agentic research")
		result, err := h.xaiClient.CreateAgenticResponse(ctx, messages, "")
		if err != nil {
			logrus.WithError(err).Error("CreateAgenticResponse failed")
			return "", err
		}

		logrus.WithField("result_keys", getKeys(result)).Info("agentic response received")

		// Extract the text from the response
		if text, ok := result["text"].(string); ok && text != "" {
			return text, nil
		}

		// Fallback: try to extract from output array
		output, ok := result["output"].([]any)
		if !ok || len(output) == 0 {
			logrus.WithField("result", result).Error("no text in agentic response")
			return "", fmt.Errorf("no text in agentic response")
		}

		// Extract content from the last message
		if msg, ok := output[len(output)-1].(map[string]any); ok {
			if content, ok := msg["content"].(string); ok {
				return content, nil
			}
		}

		// Final fallback: stringify the whole output
		outputJSON, _ := json.Marshal(result)
		return string(outputJSON), nil

	default:
		return "", fmt.Errorf("unknown server tool: %s", toolCall.Function.Name)
	}
}

// Helper to get map keys for debugging
func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (h *Handler) pingLoop(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex) {
	ticker := time.NewTicker(h.config.PingInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second))
			writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// sendMsg safely writes a JSON message to the WebSocket.
func sendMsg(conn *websocket.Conn, writeMu *sync.Mutex, msg ServerMessage) error {
	writeMu.Lock()
	defer writeMu.Unlock()
	return conn.WriteJSON(msg)
}

func makeOriginChecker(allowed []string) func(*http.Request) bool {
	if len(allowed) == 0 {
		return func(r *http.Request) bool { return true }
	}
	set := make(map[string]bool, len(allowed))
	for _, o := range allowed {
		set[o] = true
	}
	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return set[origin]
	}
}
