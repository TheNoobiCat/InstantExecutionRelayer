package config

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// Config is the top-level keeper configuration loaded from JSON.
type Config struct {
	Pyth             PythConfig      `json:"pyth"`
	Contracts        ContractsConfig `json:"contracts"`
	MonitoredMarkets []MarketConfig  `json:"monitored_markets"`
	Gas              GasConfig       `json:"gas"`
	Metrics          MetricsConfig   `json:"metrics"`
	Sync             SyncConfig      `json:"sync"`
	Execution        ExecutionConfig `json:"execution"`
}

type PythConfig struct {
	// SSE streaming endpoint (Hermes uses SSE, not WebSocket)
	StreamEndpoint string `json:"stream_endpoint"`
	// REST endpoint for fetching binary updateData for contract calls
	RestEndpoint string            `json:"rest_endpoint"`
	PriceFeeds   map[string]string `json:"price_feeds"` // symbol -> feedId (hex with 0x prefix)
}

type ContractsConfig struct {
	PythAddress  string `json:"pyth_address"`
	PerpsFactory string `json:"perps_factory"`
}

type MarketConfig struct {
	MarketSymbol   string  `json:"market_symbol"`
	MarketID       string  `json:"market_id"`
	Trading        string  `json:"trading"`          // PerpTrading contract address
	LimitOrderBook string  `json:"limit_order_book"` // LimitOrderBook contract address
	Pool           string  `json:"pool"`             // LPPool address
	PythPriceID    string  `json:"pyth_price_id"`    // Pyth feed ID (hex 0x-prefixed)
	MaxBatchSize   int     `json:"max_batch_size"`
	MinProfitUSD   float64 `json:"min_profit_usd"`
	MaxGasWei      string  `json:"max_gas_wei"`
}

type GasConfig struct {
	Multiplier         float64 `json:"multiplier"`
	MaxPriorityFeeGwei float64 `json:"max_priority_fee_gwei"`
	BaseFeeBufferGwei  float64 `json:"base_fee_buffer_gwei"`
	GasLimitBuffer     float64 `json:"gas_limit_buffer"` // multiplier for estimated gas (e.g. 1.2)
}

type MetricsConfig struct {
	PrometheusPort int `json:"prometheus_port"`
}

type SyncConfig struct {
	// How often to do a full on-chain sync of active positions/orders (seconds)
	FullSyncIntervalSec int `json:"full_sync_interval_sec"`
	// How many blocks back to scan for events on startup (ignored if using Goldsky)
	StartupLookbackBlocks uint64 `json:"startup_lookback_blocks"`
	// Goldsky GraphQL endpoint for querying active orders (optional, replaces RPC historical sync)
	GoldskyURL string `json:"goldsky_url"`
}

type ExecutionConfig struct {
	// Max concurrent pending txs per market
	MaxPendingTxs int `json:"max_pending_txs"`
	// Simulation timeout in milliseconds
	SimulationTimeoutMs int `json:"simulation_timeout_ms"`
	// Minimum seconds between retries for the same order
	RetryBackoffSec int `json:"retry_backoff_sec"`
	// Maximum number of retry attempts per order
	MaxRetries int `json:"max_retries"`
	// Pyth update data freshness: max age in seconds for binary update data
	PythUpdateMaxAgeSec int `json:"pyth_update_max_age_sec"`
}

// Load reads and parses the JSON config file at the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}

// Validate checks all required fields and returns descriptive errors.
func (c *Config) Validate() error {
	if c.Pyth.StreamEndpoint == "" {
		return fmt.Errorf("pyth.stream_endpoint is required")
	}
	if c.Pyth.RestEndpoint == "" {
		return fmt.Errorf("pyth.rest_endpoint is required")
	}
	if len(c.Pyth.PriceFeeds) == 0 {
		return fmt.Errorf("pyth.price_feeds must have at least one entry")
	}
	for sym, id := range c.Pyth.PriceFeeds {
		if !strings.HasPrefix(id, "0x") || len(id) != 66 {
			return fmt.Errorf("pyth.price_feeds[%s]: invalid feed ID %q (must be 0x + 64 hex chars)", sym, id)
		}
	}

	if len(c.MonitoredMarkets) == 0 {
		return fmt.Errorf("monitored_markets must have at least one entry")
	}
	for i, m := range c.MonitoredMarkets {
		if m.MarketSymbol == "" {
			return fmt.Errorf("monitored_markets[%d].market_symbol is required", i)
		}
		if !common.IsHexAddress(m.Trading) {
			return fmt.Errorf("monitored_markets[%d].trading: invalid address %q", i, m.Trading)
		}
		if !common.IsHexAddress(m.LimitOrderBook) {
			return fmt.Errorf("monitored_markets[%d].limit_order_book: invalid address %q", i, m.LimitOrderBook)
		}
		if m.PythPriceID == "" {
			return fmt.Errorf("monitored_markets[%d].pyth_price_id is required", i)
		}
		if m.MaxBatchSize <= 0 {
			m.MaxBatchSize = 1
		}
		if m.MaxGasWei != "" {
			if _, ok := new(big.Int).SetString(m.MaxGasWei, 10); !ok {
				return fmt.Errorf("monitored_markets[%d].max_gas_wei: invalid number %q", i, m.MaxGasWei)
			}
		}
	}

	if c.Gas.Multiplier <= 0 {
		c.Gas.Multiplier = 1.2
	}
	if c.Gas.GasLimitBuffer <= 0 {
		c.Gas.GasLimitBuffer = 1.3
	}
	if c.Metrics.PrometheusPort <= 0 {
		c.Metrics.PrometheusPort = 9091
	}
	if c.Sync.FullSyncIntervalSec <= 0 {
		c.Sync.FullSyncIntervalSec = 30
	}
	if c.Sync.StartupLookbackBlocks <= 0 {
		c.Sync.StartupLookbackBlocks = 10000
	}
	if c.Execution.MaxPendingTxs <= 0 {
		c.Execution.MaxPendingTxs = 4
	}
	if c.Execution.SimulationTimeoutMs <= 0 {
		c.Execution.SimulationTimeoutMs = 3000
	}
	if c.Execution.RetryBackoffSec <= 0 {
		c.Execution.RetryBackoffSec = 5
	}
	if c.Execution.MaxRetries <= 0 {
		c.Execution.MaxRetries = 3
	}
	if c.Execution.PythUpdateMaxAgeSec <= 0 {
		c.Execution.PythUpdateMaxAgeSec = 5
	}

	return nil
}

// GetMaxGasWei returns the max gas in wei for a market, or nil if unlimited.
func (m *MarketConfig) GetMaxGasWei() *big.Int {
	if m.MaxGasWei == "" {
		return nil
	}
	v, _ := new(big.Int).SetString(m.MaxGasWei, 10)
	return v
}
