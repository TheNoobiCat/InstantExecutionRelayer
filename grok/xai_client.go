package grok

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"
)

// XAIClient handles communication with the xAI API.
type XAIClient struct {
	apiKey     string
	httpClient *http.Client
	config     *Config
}

// NewXAIClient creates a new xAI API client.
func NewXAIClient(apiKey string, cfg *Config) *XAIClient {
	return &XAIClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: cfg.APITimeout(),
		},
		config: cfg,
	}
}

// StreamCallback is called for each chunk event during streaming.
type StreamCallback struct {
	// OnPartial is called with each content delta.
	OnPartial func(text string) error
	// OnToolCalls is called when the model finishes with tool_calls.
	OnToolCalls func(calls []ToolCall) error
	// OnComplete is called when the model finishes with a final message.
	OnComplete func(fullContent string) error
}

// StreamCompletion sends a chat completion request and streams the response.
func (c *XAIClient) StreamCompletion(ctx context.Context, history []map[string]any, cb StreamCallback) error {
	body := map[string]any{
		"model":    c.config.API.Model,
		"messages": history,
		"stream":   c.config.Chat.Stream,
	}

	// Add tools if configured and allowed by feature toggles
	allowedTools := allowedTools(c.config.Chat.Tools, c.config.Features)
	if len(allowedTools) > 0 {
		body["tools"] = allowedTools
		body["tool_choice"] = c.config.Chat.ToolChoice
	}

	// Optional parameters
	if c.config.API.MaxTokens > 0 {
		body["max_tokens"] = c.config.API.MaxTokens
	}
	if c.config.API.Temperature != nil {
		body["temperature"] = *c.config.API.Temperature
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshaling request: %w", err)
	}

	endpoint := c.config.API.BaseURL + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		logrus.WithFields(logrus.Fields{
			"status": resp.StatusCode,
			"body":   string(bodyBytes),
		}).Error("xAI API error")
		return fmt.Errorf("API error: %d — %s", resp.StatusCode, string(bodyBytes))
	}

	if !c.config.Chat.Stream {
		return c.handleNonStreaming(resp.Body, cb)
	}

	return c.handleStreaming(resp.Body, cb)
}

func (c *XAIClient) handleStreaming(body io.Reader, cb StreamCallback) error {
	scanner := bufio.NewScanner(body)
	// Increase buffer for large responses
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var accumulatedContent string
	accumulatedToolCalls := make(map[int]*ToolCall) // index -> accumulated tool call

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}

		var chunk streamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			logrus.WithError(err).Debug("failed to parse SSE chunk")
			continue
		}

		if len(chunk.Choices) == 0 {
			continue
		}

		choice := chunk.Choices[0]
		delta := choice.Delta

		// Stream content partials
		if delta.Content != nil && *delta.Content != "" {
			accumulatedContent += *delta.Content
			if cb.OnPartial != nil {
				if err := cb.OnPartial(*delta.Content); err != nil {
					return err
				}
			}
		}

		// Accumulate tool call deltas
		for _, tc := range delta.ToolCalls {
			idx := tc.Index
			existing, ok := accumulatedToolCalls[idx]
			if !ok {
				// New tool call
				newTC := ToolCall{
					Index: tc.Index,
					ID:    tc.ID,
					Type:  tc.Type,
					Function: ToolCallFunc{
						Name:      tc.Function.Name,
						Arguments: tc.Function.Arguments,
					},
				}
				accumulatedToolCalls[idx] = &newTC
			} else {
				// Merge delta — append arguments, fill in ID/name if provided
				if tc.ID != "" {
					existing.ID = tc.ID
				}
				if tc.Function.Name != "" {
					existing.Function.Name = tc.Function.Name
				}
				existing.Function.Arguments += tc.Function.Arguments
			}
		}

		// Handle finish reasons
		if choice.FinishReason != "" {
			switch choice.FinishReason {
			case "tool_calls":
				calls := toolCallMapToSlice(accumulatedToolCalls)
				if cb.OnToolCalls != nil {
					return cb.OnToolCalls(calls)
				}
			default:
				// "stop", "length", etc.
				if cb.OnComplete != nil {
					return cb.OnComplete(accumulatedContent)
				}
			}
		}
	}

	return scanner.Err()
}

func (c *XAIClient) handleNonStreaming(body io.Reader, cb StreamCallback) error {
	var resp struct {
		Choices []struct {
			Message struct {
				Role      string     `json:"role"`
				Content   string     `json:"content"`
				ToolCalls []ToolCall `json:"tool_calls,omitempty"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}

	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}

	if len(resp.Choices) == 0 {
		return fmt.Errorf("no choices in response")
	}

	choice := resp.Choices[0]

	if choice.FinishReason == "tool_calls" && len(choice.Message.ToolCalls) > 0 {
		if cb.OnToolCalls != nil {
			return cb.OnToolCalls(choice.Message.ToolCalls)
		}
	} else if cb.OnComplete != nil {
		return cb.OnComplete(choice.Message.Content)
	}

	return nil
}

// --- Internal types for SSE parsing ---

type streamChunk struct {
	Choices []streamChoice `json:"choices"`
}

type streamChoice struct {
	Delta        streamDelta `json:"delta"`
	FinishReason string      `json:"finish_reason"`
}

type streamDelta struct {
	Content   *string          `json:"content"`
	ToolCalls []streamToolCall `json:"tool_calls"`
}

type streamToolCall struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func toolCallMapToSlice(m map[int]*ToolCall) []ToolCall {
	result := make([]ToolCall, 0, len(m))
	// Iterate in index order
	for i := 0; i < len(m); i++ {
		if tc, ok := m[i]; ok {
			result = append(result, *tc)
		}
	}
	return result
}

// allowedTools enforces feature toggles before sending tool definitions to xAI.
// If both perps and options are disabled, no tools are sent. If a tool declares
// a Feature, it must be enabled to be passed through.
func allowedTools(tools []ToolDefinition, features FeaturesConfig) []ToolDefinition {
	if !features.EnablePerps && !features.EnableOptions {
		return nil
	}

	filtered := make([]ToolDefinition, 0, len(tools))
	for _, t := range tools {
		switch strings.ToLower(t.Feature) {
		case "perps":
			if !features.EnablePerps {
				continue
			}
		case "options":
			if !features.EnableOptions {
				continue
			}
		case "":
			// always allowed
		default:
			// unknown feature tag; be conservative and skip
			continue
		}
		filtered = append(filtered, t)
	}

	return filtered
}

// CreateAgenticResponse creates a new agentic response or continues an existing one.
// Automatically includes all supported xAI built-in tools.
func (c *XAIClient) CreateAgenticResponse(ctx context.Context, messages []map[string]any, previousResponseID string) (map[string]any, error) {
	body := map[string]any{
		"model":  "grok-4-1-fast-reasoning",
		"input":  messages,
		"store":  true,
		"stream": true,
	}

	if previousResponseID != "" {
		body["previous_response_id"] = previousResponseID
	}

	// Add all supported xAI built-in agentic tools
	agenticTools := []map[string]string{
		{"type": "web_search"},
		{"type": "code_execution"},
		{"type": "x_search"},
	}

	body["tools"] = agenticTools

	// Optional: request encrypted reasoning traces
	body["include"] = []string{"reasoning.encrypted_content"}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling agentic request: %w", err)
	}

	// Debug: log the full request body
	pretty, _ := json.MarshalIndent(body, "", "  ")
	logrus.Debugf("Agentic request body:\n%s", string(pretty))

	endpoint := c.config.API.BaseURL + "/responses"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		logrus.WithFields(logrus.Fields{
			"status": resp.StatusCode,
			"body":   string(bodyBytes),
		}).Error("Agentic API error")
		return nil, fmt.Errorf("agentic API error %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Parse SSE stream
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024) // Larger buffer for big responses

	var fullText string
	var responseData map[string]any

	for scanner.Scan() {
		line := scanner.Text()

		// SSE format: "data: {...}"
		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")

			var event map[string]any
			if err := json.Unmarshal([]byte(data), &event); err != nil {
				logrus.WithError(err).Debug("failed to parse SSE event")
				continue
			}

			eventType, _ := event["type"].(string)

			switch eventType {
			case "response.output_text.delta":
				// Accumulate text deltas
				if delta, ok := event["delta"].(string); ok {
					fullText += delta
				}

			case "response.output_text.done":
				// Final text is available
				if text, ok := event["text"].(string); ok {
					fullText = text
				}

			case "response.completed":
				// Store the complete response data
				responseData = event
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading SSE stream: %w", err)
	}

	// Return a structured response
	result := map[string]any{
		"text": fullText,
		"output": []map[string]any{
			{
				"role":    "assistant",
				"content": fullText,
			},
		},
	}

	if responseData != nil {
		result["response"] = responseData
	}

	logrus.WithField("text_length", len(fullText)).Info("Agentic research completed")

	return result, nil
}
