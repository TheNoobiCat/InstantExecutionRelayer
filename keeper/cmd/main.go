package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"

	"github.com/thenoobicat/instantexecutionrelayer/keeper/config"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/executor"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/metrics"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/pyth"
	keepersync "github.com/thenoobicat/instantexecutionrelayer/keeper/sync"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/trigger"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "Path to JSON config file (or set CONFIG_PATH env var)")
	flag.Parse()

	// Load .env file from parent directory (../env relative to keeper/)
	// This is safe to fail if .env doesn't exist (env vars may be set another way)
	envPath := filepath.Join("..", ".env")
	if err := godotenv.Load(envPath); err != nil {
		// Don't fatal - env vars might be set via system environment
		// We'll just log a warning
	}

	// Setup zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano}).
		With().
		Timestamp().
		Str("service", "keeper-bot").
		Logger()

	logger.Info().Msg("BrokerFi Keeper Bot starting")

	// --- Load configuration ---

	// Config path: flag > env var
	cfgPath := *configPath
	if cfgPath == "" {
		cfgPath = os.Getenv("CONFIG_PATH")
	}
	if cfgPath == "" {
		logger.Fatal().Msg("config path required: use -config flag or CONFIG_PATH env var")
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Fatal().Err(err).Str("path", cfgPath).Msg("failed to load config")
	}

	logger.Info().
		Int("markets", len(cfg.MonitoredMarkets)).
		Int("feeds", len(cfg.Pyth.PriceFeeds)).
		Msg("config loaded")

	// --- Load secrets from environment variables (NEVER from .env files) ---

	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		logger.Fatal().Msg("RPC_URL environment variable is required")
	}

	privateKeyHex := os.Getenv("RELAYER_PRIVATE_KEY")
	if privateKeyHex == "" {
		logger.Fatal().Msg("RELAYER_PRIVATE_KEY environment variable is required")
	}

	// Parse private key
	privateKeyHex = strings.TrimPrefix(privateKeyHex, "0x")
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to parse RELAYER_PRIVATE_KEY")
	}

	keeperAddr := crypto.PubkeyToAddress(privateKey.PublicKey)
	logger.Info().Str("keeper_address", keeperAddr.Hex()).Msg("keeper wallet loaded")

	// --- Connect to Ethereum ---

	ethClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		logger.Fatal().Err(err).Str("rpc", rpcURL).Msg("failed to connect to Ethereum RPC")
	}
	defer ethClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chainID, err := ethClient.ChainID(ctx)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to get chain ID")
	}
	logger.Info().Str("chain_id", chainID.String()).Msg("connected to Ethereum")

	// Check keeper balance
	balance, err := ethClient.BalanceAt(ctx, keeperAddr, nil)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to check keeper balance")
	} else {
		logger.Info().Str("balance_wei", balance.String()).Msg("keeper ETH balance")
	}

	// --- Initialize components ---

	// Metrics
	met := metrics.NewMetrics()
	metrics.Serve(cfg.Metrics.PrometheusPort, logger)

	// Executor
	exec := executor.NewExecutor(ethClient, privateKey, chainID, cfg, logger, met)

	// Collect feed IDs for Pyth subscription
	feedIDs := make([]string, 0, len(cfg.Pyth.PriceFeeds))
	for _, id := range cfg.Pyth.PriceFeeds {
		feedIDs = append(feedIDs, id)
	}

	// Trigger engine (created before Pyth client so we can wire the callback)
	var triggerEngine *trigger.Engine

	// Pyth SSE client
	pythClient := pyth.NewClient(
		cfg.Pyth.StreamEndpoint,
		cfg.Pyth.RestEndpoint,
		feedIDs,
		func(update pyth.PriceUpdate) {
			// This is called on every SSE price update
			if triggerEngine != nil {
				triggerEngine.OnPriceUpdate(update)
			}
		},
		logger,
	)

	// Wire Pyth metrics hooks
	pythClient.SetMetricsHooks(
		func(feedID string, latencyMs float64) {
			met.PythMsgLatency.WithLabelValues(feedID).Observe(latencyMs)
		},
		func() {
			met.PythReconnects.Inc()
		},
	)

	// Create trigger engine
	triggerEngine, err = trigger.NewEngine(
		ethClient,
		pythClient,
		keeperAddr,
		cfg,
		logger,
		met,
		func(result *trigger.TriggerResult) {
			exec.Execute(result)
		},
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create trigger engine")
	}

	// Get order books from trigger engine for the syncer
	// Both the trigger engine and syncer share the same order book instances
	orderBooks := triggerEngine.GetOrderBooks()

	syncer, err := keepersync.NewSyncer(ethClient, cfg, logger, met, orderBooks)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create syncer")
	}

	// --- Start all components ---

	logger.Info().Msg("starting all components")

	// Start Pyth SSE streaming
	go func() {
		if err := pythClient.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Fatal().Err(err).Msg("Pyth SSE client fatal error")
		}
	}()

	// Start order syncer (event listener + periodic sync)
	go func() {
		if err := syncer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Fatal().Err(err).Msg("syncer fatal error")
		}
	}()

	// Periodic Pyth connection status logging
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				connected := pythClient.IsConnected()
				if connected {
					met.PythConnected.Set(1)
				} else {
					met.PythConnected.Set(0)
				}

				// Log status
				totalOrders := 0
				for _, m := range cfg.MonitoredMarkets {
					book := triggerEngine.GetOrderBook(m.MarketSymbol)
					if book != nil {
						totalOrders += book.Size()
					}
				}

				logger.Info().
					Bool("pyth_connected", connected).
					Int64("reconnects", pythClient.ReconnectCount()).
					Int("active_orders", totalOrders).
					Int("pending_txs", exec.PendingCount()).
					Msg("keeper status")
			}
		}
	}()

	logger.Info().
		Str("keeper", keeperAddr.Hex()).
		Str("chain", chainID.String()).
		Int("markets", len(cfg.MonitoredMarkets)).
		Int("prometheus_port", cfg.Metrics.PrometheusPort).
		Msg("keeper bot is running")

	// --- Wait for shutdown signal ---

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info().Str("signal", sig.String()).Msg("shutdown signal received")
	cancel()

	// Give time for graceful shutdown
	time.Sleep(2 * time.Second)
	logger.Info().Msg("keeper bot stopped")
}
