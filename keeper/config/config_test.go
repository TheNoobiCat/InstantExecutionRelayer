package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

const testFeedID = "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"

func validTestConfig() *Config {
	return &Config{
		Pyth: PythConfig{
			StreamEndpoint: "https://hermes.pyth.network",
			RestEndpoint:   "https://hermes.pyth.network",
			PriceFeeds: map[string]string{
				"ETH-PERP": testFeedID,
			},
		},
		Contracts: ContractsConfig{
			PythAddress:  "0x0000000000000000000000000000000000000000",
			PerpsFactory: "0x0000000000000000000000000000000000000000",
		},
		MonitoredMarkets: []MarketConfig{
			{
				MarketSymbol:   "ETH-PERP",
				MarketID:       "eth-perp",
				Trading:        "0x1111111111111111111111111111111111111111",
				LimitOrderBook: "0x2222222222222222222222222222222222222222",
				Pool:           "0x3333333333333333333333333333333333333333",
				PythPriceID:    testFeedID,
				MaxBatchSize:   8,
				MinProfitUSD:   0,
				MaxGasWei:      "50000000000000000",
			},
		},
		Gas: GasConfig{
			Multiplier:     1.2,
			GasLimitBuffer: 1.3,
		},
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	if err := validTestConfig().Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestValidateRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "missing stream endpoint",
			mutate: func(c *Config) { c.Pyth.StreamEndpoint = "" },
			want:   "pyth.stream_endpoint",
		},
		{
			name:   "missing rest endpoint",
			mutate: func(c *Config) { c.Pyth.RestEndpoint = "" },
			want:   "pyth.rest_endpoint",
		},
		{
			name:   "missing price feeds",
			mutate: func(c *Config) { c.Pyth.PriceFeeds = nil },
			want:   "pyth.price_feeds",
		},
		{
			name: "malformed price feed id",
			mutate: func(c *Config) {
				c.Pyth.PriceFeeds["ETH-PERP"] = "0x1234"
			},
			want: "invalid feed ID",
		},
		{
			name:   "missing markets",
			mutate: func(c *Config) { c.MonitoredMarkets = nil },
			want:   "monitored_markets",
		},
		{
			name: "missing market symbol",
			mutate: func(c *Config) {
				c.MonitoredMarkets[0].MarketSymbol = ""
			},
			want: "market_symbol",
		},
		{
			name: "invalid trading address",
			mutate: func(c *Config) {
				c.MonitoredMarkets[0].Trading = "not-an-address"
			},
			want: "invalid address",
		},
		{
			name: "invalid limit order book address",
			mutate: func(c *Config) {
				c.MonitoredMarkets[0].LimitOrderBook = "0x1234"
			},
			want: "invalid address",
		},
		{
			name: "missing pyth price id",
			mutate: func(c *Config) {
				c.MonitoredMarkets[0].PythPriceID = ""
			},
			want: "pyth_price_id",
		},
		{
			name: "invalid max gas",
			mutate: func(c *Config) {
				c.MonitoredMarkets[0].MaxGasWei = "not-a-number"
			},
			want: "max_gas_wei",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			tt.mutate(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestLoadReadsAndValidatesConfig(t *testing.T) {
	path := t.TempDir() + "/keeper.json"
	data, err := json.Marshal(validTestConfig())
	if err != nil {
		t.Fatalf("marshal test config: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write test config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if got := cfg.MonitoredMarkets[0].MarketSymbol; got != "ETH-PERP" {
		t.Fatalf("market symbol = %q, want ETH-PERP", got)
	}
	if len(cfg.Pyth.PriceFeeds) != 1 {
		t.Fatalf("price feeds = %d, want 1", len(cfg.Pyth.PriceFeeds))
	}
}

func TestGetMaxGasWei(t *testing.T) {
	market := &MarketConfig{MaxGasWei: "50000000000000000"}
	got := market.GetMaxGasWei()
	if got == nil || got.String() != "50000000000000000" {
		t.Fatalf("GetMaxGasWei() = %v, want 50000000000000000", got)
	}

	empty := &MarketConfig{}
	if got := empty.GetMaxGasWei(); got != nil {
		t.Fatalf("GetMaxGasWei() for empty value = %v, want nil", got)
	}
}
