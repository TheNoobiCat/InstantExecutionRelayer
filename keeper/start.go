package keeper

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/rs/zerolog"

	"github.com/thenoobicat/instantexecutionrelayer/keeper/config"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/executor"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/metrics"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/pyth"
	keepersync "github.com/thenoobicat/instantexecutionrelayer/keeper/sync"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/trigger"
)

// Start initializes and runs the keeper bot in the background
// Returns a context cancel function that can be used to shut down the keeper
func Start(parentCtx context.Context, logger zerolog.Logger) (context.CancelFunc, error) {
	logger.Info().Msg("BrokerFi Keeper Bot starting")

	// --- Load configuration ---

	cfgPath := os.Getenv("CONFIG_PATH")
	if cfgPath == "" {
		logger.Warn().Msg("CONFIG_PATH not set - keeper will not start (this is optional)")
		return func() {}, nil
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Error().Err(err).Str("path", cfgPath).Msg("failed to load keeper config - keeper will not start")
		return func() {}, nil
	}

	logger.Info().
		Int("markets", len(cfg.MonitoredMarkets)).
		Int("feeds", len(cfg.Pyth.PriceFeeds)).
		Msg("keeper config loaded")

	// --- Load secrets from environment variables ---

	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		logger.Error().Msg("RPC_URL environment variable is required for keeper")
		return func() {}, nil
	}

	privateKeyHex := os.Getenv("RELAYER_PRIVATE_KEY")
	if privateKeyHex == "" {
		logger.Error().Msg("RELAYER_PRIVATE_KEY environment variable is required for keeper")
		return func() {}, nil
	}

	// Parse private key
	privateKeyHex = strings.TrimPrefix(privateKeyHex, "0x")
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		logger.Error().Err(err).Msg("failed to parse RELAYER_PRIVATE_KEY for keeper")
		return func() {}, nil
	}

	keeperAddr := crypto.PubkeyToAddress(privateKey.PublicKey)
	logger.Info().Str("keeper_address", keeperAddr.Hex()).Msg("keeper wallet loaded")

	// --- Connect to Ethereum ---

	ethClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		logger.Error().Err(err).Str("rpc", rpcURL).Msg("failed to connect to Ethereum RPC for keeper")
		return func() {}, nil
	}

	ctx, cancel := context.WithCancel(parentCtx)

	chainID, err := ethClient.ChainID(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("failed to get chain ID for keeper")
		ethClient.Close()
		cancel()
		return func() {}, nil
	}
	logger.Info().Str("chain_id", chainID.String()).Msg("keeper connected to Ethereum")

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
	go metrics.Serve(cfg.Metrics.PrometheusPort, logger)

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
		logger.Error().Err(err).Msg("failed to create trigger engine")
		ethClient.Close()
		cancel()
		return func() {}, nil
	}

	// Get order books from trigger engine for the syncer
	orderBooks := triggerEngine.GetOrderBooks()

	syncer, err := keepersync.NewSyncer(ethClient, cfg, logger, met, orderBooks)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create syncer")
		ethClient.Close()
		cancel()
		return func() {}, nil
	}

	// --- Start all components in goroutines ---

	logger.Info().Msg("starting all keeper components")

	// Start Pyth SSE streaming
	go func() {
		if err := pythClient.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error().Err(err).Msg("Pyth SSE client fatal error")
		}
	}()

	// Start order syncer (event listener + periodic sync)
	go func() {
		if err := syncer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error().Err(err).Msg("syncer fatal error")
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

	// Return a cleanup function
	cleanupFunc := func() {
		logger.Info().Msg("shutting down keeper")
		cancel()
		ethClient.Close()
		time.Sleep(2 * time.Second)
		logger.Info().Msg("keeper bot stopped")
	}

	return cleanupFunc, nil
}
