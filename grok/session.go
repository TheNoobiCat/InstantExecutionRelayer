package grok

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// SessionManager tracks active sessions, enforces per-IP limits, and manages
// captcha re-verification state.
type SessionManager struct {
	mu             sync.Mutex
	config         *Config
	sessionsPerIP  map[string]int32
	globalSessions atomic.Int32
}

// Session holds per-connection state for a Grok chat session.
type Session struct {
	ID              string
	IP              string
	CaptchaVerified bool
	CaptchaTime     time.Time
	MessageCount    int
	LastMessageTime time.Time
	History         []map[string]any

	// PendingMessage holds a user message that was blocked by captcha
	// re-verification. After the user solves the captcha, this message
	// is automatically replayed so the conversation continues.
	PendingMessage string

	manager *SessionManager
}

// NewSessionManager creates a session manager with the given config.
func NewSessionManager(cfg *Config) *SessionManager {
	return &SessionManager{
		config:        cfg,
		sessionsPerIP: make(map[string]int32),
	}
}

// CreateSession attempts to create a new session for the given request.
// Returns nil and an error string if limits are exceeded.
func (sm *SessionManager) CreateSession(r *http.Request) (*Session, string) {
	ip := extractIP(r)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check global limit
	if sm.config.Session.MaxGlobalSessions > 0 {
		if int(sm.globalSessions.Load()) >= sm.config.Session.MaxGlobalSessions {
			logrus.Warn("global session limit reached")
			return nil, "server is at capacity, try again later"
		}
	}

	// Check per-IP limit
	if sm.config.Session.MaxSessionsPerIP > 0 {
		if int(sm.sessionsPerIP[ip]) >= sm.config.Session.MaxSessionsPerIP {
			logrus.WithField("ip", ip).Warn("per-IP session limit reached")
			return nil, "too many active sessions from your IP"
		}
	}

	sm.sessionsPerIP[ip]++
	sm.globalSessions.Add(1)

	sess := &Session{
		ID:      generateSessionID(),
		IP:      ip,
		History: []map[string]any{},
		manager: sm,
	}

	// Prepend system prompt if configured
	if sm.config.Chat.SystemPrompt != "" {
		sess.History = append(sess.History, map[string]any{
			"role":    "system",
			"content": sm.config.Chat.SystemPrompt,
		})
	}

	logrus.WithFields(logrus.Fields{
		"session": sess.ID,
		"ip":      ip,
	}).Info("grok session created")

	return sess, ""
}

// DestroySession cleans up session counters.
func (sm *SessionManager) DestroySession(sess *Session) {
	if sess == nil {
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.sessionsPerIP[sess.IP]--
	if sm.sessionsPerIP[sess.IP] <= 0 {
		delete(sm.sessionsPerIP, sess.IP)
	}
	sm.globalSessions.Add(-1)

	logrus.WithField("session", sess.ID).Info("grok session destroyed")
}

// NeedsCaptcha checks whether the session requires (re-)captcha verification.
func (sess *Session) NeedsCaptcha(cfg *Config) bool {
	if !cfg.Captcha.Enabled {
		return false
	}

	// Never verified
	if !sess.CaptchaVerified {
		return true
	}

	// Time-based re-verification
	if cfg.Captcha.ReVerifyAfterSeconds > 0 {
		if time.Since(sess.CaptchaTime) > cfg.ReVerifyDuration() {
			return true
		}
	}

	// Message-count-based re-verification
	if cfg.Captcha.ReVerifyAfterMessages > 0 {
		if sess.MessageCount >= cfg.Captcha.ReVerifyAfterMessages {
			return true
		}
	}

	return false
}

// MarkCaptchaVerified records successful captcha verification and resets
// the message counter.
func (sess *Session) MarkCaptchaVerified() {
	sess.CaptchaVerified = true
	sess.CaptchaTime = time.Now()
	sess.MessageCount = 0
}

// CanSendMessage checks rate limiting. Returns true if OK.
func (sess *Session) CanSendMessage(cfg *Config) bool {
	if cfg.Session.MinMessageIntervalSeconds <= 0 {
		return true
	}
	return time.Since(sess.LastMessageTime) >= cfg.MinMessageInterval()
}

// AddUserMessage appends a user message, trims history if needed.
func (sess *Session) AddUserMessage(content string, cfg *Config) {
	sess.History = append(sess.History, map[string]any{
		"role":    "user",
		"content": content,
	})
	sess.MessageCount++
	sess.LastMessageTime = time.Now()
	sess.trimHistory(cfg)
}

// AddAssistantMessage appends an assistant message.
func (sess *Session) AddAssistantMessage(content string, cfg *Config) {
	sess.History = append(sess.History, map[string]any{
		"role":    "assistant",
		"content": content,
	})
	sess.trimHistory(cfg)
}

// AddAssistantToolCalls appends an assistant message with tool calls.
func (sess *Session) AddAssistantToolCalls(toolCalls []ToolCall, cfg *Config) {
	// Build the tool_calls array in xAI format
	calls := make([]map[string]any, len(toolCalls))
	for i, tc := range toolCalls {
		calls[i] = map[string]any{
			"id":   tc.ID,
			"type": tc.Type,
			"function": map[string]any{
				"name":      tc.Function.Name,
				"arguments": tc.Function.Arguments,
			},
		}
	}
	sess.History = append(sess.History, map[string]any{
		"role":       "assistant",
		"tool_calls": calls,
	})
	sess.trimHistory(cfg)
}

// AddToolResult appends a tool result message.
func (sess *Session) AddToolResult(toolCallID, name, content string, cfg *Config) {
	sess.History = append(sess.History, map[string]any{
		"role":         "tool",
		"tool_call_id": toolCallID,
		"name":         name,
		"content":      content,
	})
	sess.trimHistory(cfg)
}

// ClearHistory resets conversation, keeping only system prompt.
func (sess *Session) ClearHistory(cfg *Config) {
	sess.History = []map[string]any{}
	if cfg.Chat.SystemPrompt != "" {
		sess.History = append(sess.History, map[string]any{
			"role":    "system",
			"content": cfg.Chat.SystemPrompt,
		})
	}
}

func (sess *Session) trimHistory(cfg *Config) {
	max := cfg.Session.MaxHistoryLength
	if max <= 0 || len(sess.History) <= max {
		return
	}

	// Keep system prompt at index 0 if present
	start := 0
	if len(sess.History) > 0 {
		if role, ok := sess.History[0]["role"].(string); ok && role == "system" {
			start = 1
		}
	}

	excess := len(sess.History) - max
	if excess > 0 && start < len(sess.History) {
		// Remove oldest non-system messages
		sess.History = append(sess.History[:start], sess.History[start+excess:]...)
	}
}

// --- Helpers ---

func extractIP(r *http.Request) string {
	// Check common proxy headers
	if cfIP := r.Header.Get("CF-Connecting-IP"); cfIP != "" {
		return cfIP
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func generateSessionID() string {
	// Simple timestamp + random-ish ID
	return time.Now().Format("20060102150405") + "-" + randomHex(8)
}

func randomHex(n int) string {
	const hex = "0123456789abcdef"
	b := make([]byte, n)
	// Use time-based seed for simplicity; crypto/rand if you want stronger
	t := time.Now().UnixNano()
	for i := range b {
		b[i] = hex[(t>>uint(i*4))&0xf]
	}
	return string(b)
}
