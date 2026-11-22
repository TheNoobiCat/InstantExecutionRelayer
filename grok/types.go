package grok

// --- WebSocket Messages (client <-> server) ---

// ClientMessage is what the client sends over the WebSocket.
type ClientMessage struct {
	Type    string       `json:"type"`              // "captcha", "message", "tool_results", "clear"
	Content string       `json:"content,omitempty"` // For "message"
	Token   string       `json:"token,omitempty"`   // For "captcha" (Turnstile token)
	Results []ToolResult `json:"results,omitempty"` // For "tool_results"
}

// ServerMessage is what the server sends over the WebSocket.
type ServerMessage struct {
	Type     string     `json:"type"`                // "captcha_required", "captcha_ok", "partial", "message", "tool_calls", "tool_executing", "tool_complete", "error", "info"
	Content  string     `json:"content,omitempty"`   // Text content
	Calls    []ToolCall `json:"calls,omitempty"`     // For "tool_calls" and "tool_executing"
	ToolName string     `json:"tool_name,omitempty"` // For "tool_executing" and "tool_complete"
}

// ToolCall represents a tool call from the xAI API.
type ToolCall struct {
	Index    int          `json:"index"`
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function ToolCallFunc `json:"function"`
}

type ToolCallFunc struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolResult is a client-submitted tool execution result.
type ToolResult struct {
	ToolCallID string `json:"tool_call_id"`
	Name       string `json:"name"`
	Content    string `json:"content"`
}
