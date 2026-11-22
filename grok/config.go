package grok

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// Config holds the entire Grok service configuration, loaded from JSON.
type Config struct {
	// xAI API settings
	API APIConfig `json:"api"`

	// Cloudflare Turnstile captcha settings
	Captcha CaptchaConfig `json:"captcha"`

	// Feature toggles
	Features FeaturesConfig `json:"features"`

	// Session and rate limiting
	Session SessionConfig `json:"session"`

	// Chat behaviour
	Chat ChatConfig `json:"chat"`

	// WebSocket settings
	WebSocket WebSocketConfig `json:"websocket"`
}

type APIConfig struct {
	// Model to use (e.g. "grok-4", "grok-3")
	Model string `json:"model"`

	// Base URL for xAI API
	BaseURL string `json:"base_url"`

	// Request timeout
	TimeoutSeconds int `json:"timeout_seconds"`

	// Max tokens per response (0 = API default)
	MaxTokens int `json:"max_tokens,omitempty"`

	// Temperature (nil = API default)
	Temperature *float64 `json:"temperature,omitempty"`
}

type CaptchaConfig struct {
	// Enable/disable captcha entirely
	Enabled bool `json:"enabled"`

	// Re-verify captcha after this many seconds of session activity
	ReVerifyAfterSeconds int `json:"re_verify_after_seconds"`

	// Re-verify after this many messages sent (0 = disabled)
	ReVerifyAfterMessages int `json:"re_verify_after_messages"`

	// Turnstile verify endpoint
	VerifyURL string `json:"verify_url"`
}

type SessionConfig struct {
	// Maximum conversation history length (messages). Oldest trimmed first.
	MaxHistoryLength int `json:"max_history_length"`

	// Session idle timeout seconds (disconnect after inactivity)
	IdleTimeoutSeconds int `json:"idle_timeout_seconds"`

	// Max concurrent sessions per IP
	MaxSessionsPerIP int `json:"max_sessions_per_ip"`

	// Global max concurrent sessions (0 = unlimited)
	MaxGlobalSessions int `json:"max_global_sessions"`

	// Rate limit: min seconds between user messages
	MinMessageIntervalSeconds int `json:"min_message_interval_seconds"`
}

// FeaturesConfig controls which capability sections and tools are enabled.
type FeaturesConfig struct {
	EnablePerps       bool `json:"enable_perps"`
	EnableOptions     bool `json:"enable_options"`
	EnablePolymarket  bool `json:"enable_polymarket"`
	EnableAutonomous  bool `json:"enable_autonomous"`
	EnableHypeWins    bool `json:"enable_hype_wins"`
	EnableChaoticLoss bool `json:"enable_chaotic_loss"`

	DefaultRiskPerTrade float64 `json:"default_risk_per_trade"`
	MaxPortfolioRisk    float64 `json:"max_portfolio_risk"`
}

type ChatConfig struct {
	// System prompt prepended to every conversation
	SystemPrompt string `json:"system_prompt"`

	// Optional path to a markdown prompt file; if set, overrides SystemPrompt and
	// will be preprocessed using feature toggles.
	SystemPromptPath string `json:"system_prompt_path"`

	// Whether streaming is enabled
	Stream bool `json:"stream"`

	// Tool choice mode: "auto", "none", "required"
	ToolChoice string `json:"tool_choice"`

	// Available tools (xAI function calling format)
	Tools []ToolDefinition `json:"tools"`
}

type ToolDefinition struct {
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
	// Optional feature flag: "perps" or "options". Empty means always allowed.
	Feature string `json:"feature,omitempty"`
}

type ToolFunction struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"`
}

type WebSocketConfig struct {
	// Read buffer size
	ReadBufferSize int `json:"read_buffer_size"`

	// Write buffer size
	WriteBufferSize int `json:"write_buffer_size"`

	// Max message size from client (bytes)
	MaxMessageSize int64 `json:"max_message_size"`

	// Ping interval seconds
	PingIntervalSeconds int `json:"ping_interval_seconds"`

	// Pong wait seconds
	PongWaitSeconds int `json:"pong_wait_seconds"`

	// Allowed origins (empty = allow all — not recommended for prod)
	AllowedOrigins []string `json:"allowed_origins"`
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig() *Config {
	return &Config{
		API: APIConfig{
			Model:          "grok-4",
			BaseURL:        "https://api.x.ai/v1",
			TimeoutSeconds: 120,
		},
		Captcha: CaptchaConfig{
			Enabled:               true,
			ReVerifyAfterSeconds:  600, // Re-verify every 10 minutes
			ReVerifyAfterMessages: 50,  // Re-verify every 50 messages
			VerifyURL:             "https://challenges.cloudflare.com/turnstile/v0/siteverify",
		},
		Features: FeaturesConfig{
			EnablePerps:         true,
			EnableOptions:       true,
			EnablePolymarket:    false,
			EnableAutonomous:    true,
			EnableHypeWins:      true,
			EnableChaoticLoss:   true,
			DefaultRiskPerTrade: 1.5,
			MaxPortfolioRisk:    10.0,
		},
		Session: SessionConfig{
			MaxHistoryLength:          100,
			IdleTimeoutSeconds:        300,
			MaxSessionsPerIP:          3,
			MaxGlobalSessions:         0,
			MinMessageIntervalSeconds: 2,
		},
		Chat: ChatConfig{
			SystemPrompt:     "You are a helpful AI assistant.",
			SystemPromptPath: "SYSTEM_PROMPT.md",
			Stream:           true,
			ToolChoice:       "auto",
			Tools:            []ToolDefinition{},
		},
		WebSocket: WebSocketConfig{
			ReadBufferSize:      1024,
			WriteBufferSize:     1024,
			MaxMessageSize:      8192,
			PingIntervalSeconds: 30,
			PongWaitSeconds:     60,
			AllowedOrigins:      []string{},
		},
	}
}

// LoadConfig reads a JSON config file and merges with defaults.
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			logrus.Warnf("grok config not found at %s, using defaults", path)
			return cfg, nil
		}
		return nil, fmt.Errorf("reading config: %w", err)
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	logrus.Infof("loaded grok config from %s", path)
	return cfg, nil
}

// Helper durations
func (c *Config) APITimeout() time.Duration {
	return time.Duration(c.API.TimeoutSeconds) * time.Second
}

func (c *Config) IdleTimeout() time.Duration {
	return time.Duration(c.Session.IdleTimeoutSeconds) * time.Second
}

func (c *Config) PingInterval() time.Duration {
	return time.Duration(c.WebSocket.PingIntervalSeconds) * time.Second
}

func (c *Config) PongWait() time.Duration {
	return time.Duration(c.WebSocket.PongWaitSeconds) * time.Second
}

func (c *Config) ReVerifyDuration() time.Duration {
	return time.Duration(c.Captcha.ReVerifyAfterSeconds) * time.Second
}

func (c *Config) MinMessageInterval() time.Duration {
	return time.Duration(c.Session.MinMessageIntervalSeconds) * time.Second
}
