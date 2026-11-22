package pyth

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// PriceUpdate represents a parsed Pyth price update from the SSE stream.
type PriceUpdate struct {
	FeedID      string   // 0x-prefixed hex feed ID
	Price       *big.Int // raw price integer
	Conf        *big.Int // raw confidence integer
	Expo        int32    // exponent
	PublishTime int64    // unix timestamp
	// Derived float price = Price * 10^Expo
	PriceFloat float64
	// The raw binary hex data for this update (for building contract updateData)
	BinaryHex  string
	ReceivedAt time.Time
}

// PriceCallback is called on each price update received from the SSE stream.
type PriceCallback func(update PriceUpdate)

// SSEStreamResponse is the JSON structure of each SSE data event from Hermes.
type SSEStreamResponse struct {
	Binary struct {
		Encoding string   `json:"encoding"`
		Data     []string `json:"data"`
	} `json:"binary"`
	Parsed []ParsedPrice `json:"parsed"`
}

// ParsedPrice is a single parsed price from the Hermes response.
type ParsedPrice struct {
	ID    string `json:"id"`
	Price struct {
		Price       string `json:"price"`
		Conf        string `json:"conf"`
		Expo        int32  `json:"expo"`
		PublishTime int64  `json:"publish_time"`
	} `json:"price"`
	EMAPrice struct {
		Price       string `json:"price"`
		Conf        string `json:"conf"`
		Expo        int32  `json:"expo"`
		PublishTime int64  `json:"publish_time"`
	} `json:"ema_price"`
}

// Client manages SSE streaming connections to Pyth Hermes.
// It mirrors the BrokerFi frontend's approach: SSE via /v2/updates/price/stream
// and REST via /v2/updates/price/latest for binary updateData.
type Client struct {
	streamEndpoint string
	restEndpoint   string
	feedIDs        []string // 0x-prefixed hex feed IDs
	callback       PriceCallback
	logger         zerolog.Logger
	httpClient     *http.Client

	// Latest binary update data per feed, protected by mutex
	mu           sync.RWMutex
	latestBinary map[string]*BinaryUpdate // feedID -> latest binary data

	// Connection state
	connected      atomic.Bool
	reconnectCount atomic.Int64

	// Metrics hooks
	onMsgLatency func(feedID string, latencyMs float64)
	onReconnect  func()
}

// BinaryUpdate holds the most recent binary price update data for a feed.
type BinaryUpdate struct {
	HexData     string // hex-encoded binary data (without 0x prefix)
	PublishTime int64
	ReceivedAt  time.Time
}

// NewClient creates a new Pyth SSE streaming client.
func NewClient(streamEndpoint, restEndpoint string, feedIDs []string, callback PriceCallback, logger zerolog.Logger) *Client {
	// Normalize feed IDs: ensure 0x prefix, lowercase
	normalized := make([]string, len(feedIDs))
	for i, id := range feedIDs {
		id = strings.TrimPrefix(strings.ToLower(id), "0x")
		normalized[i] = "0x" + id
	}

	return &Client{
		streamEndpoint: strings.TrimRight(streamEndpoint, "/"),
		restEndpoint:   strings.TrimRight(restEndpoint, "/"),
		feedIDs:        normalized,
		callback:       callback,
		logger:         logger.With().Str("component", "pyth-sse").Logger(),
		httpClient: &http.Client{
			Timeout: 0, // SSE streams have no timeout
		},
		latestBinary: make(map[string]*BinaryUpdate),
	}
}

// SetMetricsHooks sets optional callbacks for metrics collection.
func (c *Client) SetMetricsHooks(onMsgLatency func(string, float64), onReconnect func()) {
	c.onMsgLatency = onMsgLatency
	c.onReconnect = onReconnect
}

// IsConnected returns whether the SSE stream is currently connected.
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// ReconnectCount returns the total number of reconnections since start.
func (c *Client) ReconnectCount() int64 {
	return c.reconnectCount.Load()
}

// Run starts the SSE streaming connection with automatic reconnection.
// It blocks until the context is cancelled.
// Uses exponential backoff with jitter on reconnect, mirroring best practices.
func (c *Client) Run(ctx context.Context) error {
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			c.connected.Store(false)
			return ctx.Err()
		default:
		}

		err := c.connectAndStream(ctx)
		c.connected.Store(false)

		if ctx.Err() != nil {
			return ctx.Err()
		}

		c.reconnectCount.Add(1)
		if c.onReconnect != nil {
			c.onReconnect()
		}

		// Exponential backoff with jitter (mirrors robust reconnection patterns)
		jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
		sleepDur := backoff + jitter
		c.logger.Warn().Err(err).Dur("backoff", sleepDur).Msg("SSE stream disconnected, reconnecting")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDur):
		}

		// Increase backoff, capped at max
		backoff = time.Duration(float64(backoff) * 1.5)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// connectAndStream establishes one SSE connection and reads events until error or context cancel.
func (c *Client) connectAndStream(ctx context.Context) error {
	// Build SSE URL exactly as BrokerFi frontend does:
	// https://hermes.pyth.network/v2/updates/price/stream?ids[]=FEEDID&parsed=true&binary=true
	// We request both parsed (for price data) and binary (for contract updateData)
	url := c.streamEndpoint + "/v2/updates/price/stream?"
	params := make([]string, 0, len(c.feedIDs)+2)
	for _, id := range c.feedIDs {
		// Strip 0x prefix for the URL parameter (Hermes accepts both formats,
		// but BrokerFi frontend strips it then re-adds)
		cleanID := strings.TrimPrefix(id, "0x")
		params = append(params, "ids[]=0x"+cleanID)
	}
	params = append(params, "parsed=true", "binary=true")
	url += strings.Join(params, "&")

	c.logger.Info().Str("url", url).Int("feeds", len(c.feedIDs)).Msg("connecting to Pyth SSE stream")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create SSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("SSE connect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("SSE stream returned %d: %s", resp.StatusCode, string(body))
	}

	c.connected.Store(true)
	c.logger.Info().Msg("SSE stream connected")

	// Reset backoff on successful connection
	scanner := bufio.NewScanner(resp.Body)
	// SSE events can be large (binary data), increase buffer
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()
		// SSE format: "data:{json}\n\n"
		if !strings.HasPrefix(line, "data:") {
			continue
		}

		data := strings.TrimPrefix(line, "data:")
		receivedAt := time.Now()

		var sseResp SSEStreamResponse
		if err := json.Unmarshal([]byte(data), &sseResp); err != nil {
			c.logger.Debug().Err(err).Str("data", truncate(data, 200)).Msg("failed to parse SSE event")
			continue
		}

		c.processSSEEvent(&sseResp, receivedAt)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("SSE stream read: %w", err)
	}

	return fmt.Errorf("SSE stream ended (EOF)")
}

// processSSEEvent processes a single SSE event containing parsed prices and binary data.
func (c *Client) processSSEEvent(resp *SSEStreamResponse, receivedAt time.Time) {
	// Store binary data (for building contract updateData)
	var binaryHex string
	if len(resp.Binary.Data) > 0 {
		binaryHex = resp.Binary.Data[0] // Use the first binary blob
	}

	for _, parsed := range resp.Parsed {
		feedID := "0x" + strings.ToLower(strings.TrimPrefix(parsed.ID, "0x"))

		// Parse price and confidence as big.Int (matching BrokerFi's approach)
		price := new(big.Int)
		price.SetString(parsed.Price.Price, 10)

		conf := new(big.Int)
		conf.SetString(parsed.Price.Conf, 10)

		// Calculate float price exactly as BrokerFi frontend does:
		// realPrice = parseInt(price_string) * 10^expo
		priceFloat := float64(price.Int64()) * math.Pow(10, float64(parsed.Price.Expo))

		update := PriceUpdate{
			FeedID:      feedID,
			Price:       price,
			Conf:        conf,
			Expo:        parsed.Price.Expo,
			PublishTime: parsed.Price.PublishTime,
			PriceFloat:  priceFloat,
			BinaryHex:   binaryHex,
			ReceivedAt:  receivedAt,
		}

		// Store latest binary data for this feed
		if binaryHex != "" {
			c.mu.Lock()
			c.latestBinary[feedID] = &BinaryUpdate{
				HexData:     binaryHex,
				PublishTime: parsed.Price.PublishTime,
				ReceivedAt:  receivedAt,
			}
			c.mu.Unlock()
		}

		// Report latency metric
		if c.onMsgLatency != nil {
			latencyMs := float64(receivedAt.UnixMilli() - parsed.Price.PublishTime*1000)
			c.onMsgLatency(feedID, latencyMs)
		}

		// Invoke callback
		if c.callback != nil {
			c.callback(update)
		}
	}
}

// GetLatestBinaryUpdateData returns the most recent binary update data for contract calls.
// Returns hex strings with 0x prefix, ready to be converted to bytes for ABI encoding.
// This mirrors BrokerFi's getPythUpdateData() which fetches from /v2/updates/price/latest?binary=true
func (c *Client) GetLatestBinaryUpdateData(feedID string, maxAgeSec int64) ([]byte, error) {
	feedID = "0x" + strings.ToLower(strings.TrimPrefix(feedID, "0x"))

	c.mu.RLock()
	bu, ok := c.latestBinary[feedID]
	c.mu.RUnlock()

	if ok && time.Since(bu.ReceivedAt).Seconds() <= float64(maxAgeSec) {
		// Use cached binary data from stream
		return hex.DecodeString(bu.HexData)
	}

	// Fallback: fetch fresh binary data from REST endpoint
	// This mirrors BrokerFi's getPythUpdateData() function exactly
	return c.FetchBinaryUpdateData(feedID)
}

// FetchBinaryUpdateData fetches binary price update data from Hermes REST API.
// This is used for building the bytes[] priceUpdateData argument for contract calls.
// Mirrors BrokerFi's getPythUpdateData() in pythPriceMulti.ts
func (c *Client) FetchBinaryUpdateData(feedID string) ([]byte, error) {
	cleanID := strings.TrimPrefix(feedID, "0x")
	url := fmt.Sprintf("%s/v2/updates/price/latest?ids[]=0x%s&binary=true", c.restEndpoint, cleanID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Cache-Control", "no-store")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch binary update data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("hermes returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Binary struct {
			Encoding string      `json:"encoding"`
			Data     interface{} `json:"data"`
		} `json:"binary"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Handle both array and string responses (matching BrokerFi's logic)
	var hexStr string
	switch d := result.Binary.Data.(type) {
	case []interface{}:
		if len(d) > 0 {
			hexStr, _ = d[0].(string)
		}
	case string:
		hexStr = d
	default:
		return nil, fmt.Errorf("unexpected binary data type: %T", result.Binary.Data)
	}

	if hexStr == "" {
		return nil, fmt.Errorf("no binary data returned for feed %s", feedID)
	}

	return hex.DecodeString(hexStr)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
