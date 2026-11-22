package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/sirupsen/logrus"
	"github.com/thenoobicat/instantexecutionrelayer/endpoints"
	"github.com/thenoobicat/instantexecutionrelayer/faucet"
	"github.com/thenoobicat/instantexecutionrelayer/grok"
	"github.com/thenoobicat/instantexecutionrelayer/keeper"
	"github.com/thenoobicat/instantexecutionrelayer/leaderboard"
	"github.com/thenoobicat/instantexecutionrelayer/relayer"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var (
	globalRelayer *relayer.Relayer
	relayerMutex  sync.Mutex
)

func getGlobalRelayer() *relayer.Relayer {
	relayerMutex.Lock()
	defer relayerMutex.Unlock()

	if globalRelayer == nil {
		// Load environment variables
		_ = godotenv.Load()

		rpcURL := os.Getenv("RPC_URL")
		if rpcURL == "" {
			logrus.Fatal("RPC_URL not set in environment")
		}

		factoryAddrHex := os.Getenv("FACTORY_ADDRESS")
		if factoryAddrHex == "" {
			logrus.Fatal("FACTORY_ADDRESS not set in environment")
		}

		// Connect to Ethereum client
		client, err := ethclient.Dial(rpcURL)
		if err != nil {
			logrus.WithError(err).Error("Failed to connect to Ethereum client")
			return nil
		}

		// Get chain ID from client
		chainID, err := client.ChainID(context.Background())
		if err != nil {
			logrus.WithError(err).Error("Failed to get chain ID")
			return nil
		}

		// Create relayer with dynamic chain ID and factory address
		globalRelayer = relayer.NewRelayer(client, chainID, common.HexToAddress(factoryAddrHex))

		// Initialize leaderboard database
		leaderboardPath := os.Getenv("LEADERBOARD_DB_PATH")
		if leaderboardPath == "" {
			leaderboardPath = "leaderboard.db"
		}
		ldb, err := leaderboard.Open(leaderboardPath)
		if err != nil {
			logrus.WithError(err).Warn("Failed to open leaderboard database - agent tracking will be disabled")
		} else {
			// Configure USDC address for profit calculation
			usdcAddrHex := os.Getenv("USDC_ADDRESS")
			if usdcAddrHex != "" {
				ldb.SetClient(client, common.HexToAddress(usdcAddrHex), common.HexToAddress(factoryAddrHex))
				logrus.WithFields(logrus.Fields{
					"usdc_address":    usdcAddrHex,
					"factory_address": factoryAddrHex,
				}).Info("Leaderboard configured with USDC and factory addresses for profit tracking")
			} else {
				logrus.Warn("USDC_ADDRESS not set - profit calculation will be disabled")
			}

			globalRelayer.SetLeaderboardDB(ldb)
			logrus.WithField("path", leaderboardPath).Info("Leaderboard database initialized")
		}

		logrus.Info("Global relayer initialized")
	}

	return globalRelayer
}

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "", "Path to keeper config JSON file (or set CONFIG_PATH env var)")
	flag.Parse()

	// Load .env early so all os.Getenv calls below see the values
	_ = godotenv.Load()

	// If -config flag is provided, set CONFIG_PATH env var so keeper can use it
	if *configPath != "" {
		os.Setenv("CONFIG_PATH", *configPath)
		logrus.WithField("config", *configPath).Info("Keeper config path set from command-line flag")
	}

	// Configure logging level from environment (default: warn)
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "warn"
	}

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logrus.SetLevel(logrus.WarnLevel)
	} else {
		logrus.SetLevel(level)
	}

	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetOutput(os.Stderr)

	// --- Start Keeper Bot in background (if CONFIG_PATH is set) ---
	keeperCtx, keeperCancel := context.WithCancel(context.Background())
	defer keeperCancel()

	// Setup zerolog for keeper
	zeroLogger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		With().
		Timestamp().
		Str("service", "keeper-bot").
		Logger()

	keeperCleanup, err := keeper.Start(keeperCtx, zeroLogger)
	if err != nil {
		logrus.WithError(err).Warn("Keeper failed to start - continuing without keeper")
	} else {
		defer keeperCleanup()
		logrus.Info("Keeper bot started successfully")
	}

	// --- Setup HTTP endpoints ---

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logrus.WithError(err).Warn("WebSocket upgrade failed")
			return
		}
		// Handle each connection in its own goroutine; the handler will
		// close the connection when done.
		go endpoints.HandleConnection(conn, getGlobalRelayer())
	})

	// --- Grok AI Chat endpoint ---
	grokConfigPath := os.Getenv("GROK_CONFIG_PATH")
	if grokConfigPath == "" {
		grokConfigPath = "grok_config.json"
	}
	grokCfg, err := grok.LoadConfig(grokConfigPath)
	if err != nil {
		logrus.WithError(err).Fatal("failed to load grok config")
	}

	// Load system prompt from file (if configured) and apply feature filters
	grokCfg.Chat.SystemPrompt = grok.LoadAndPreparePrompt(grokCfg)

	// Initialize global relayer early so all logs appear
	logrus.Info("Initializing global relayer...")
	relayer := getGlobalRelayer()
	if relayer == nil {
		logrus.Fatal("Failed to initialize global relayer")
	}

	// XAI API key (for Grok) and register leaderboard endpoint if available
	xaiKey := os.Getenv("XAI_API_KEY")
	if relayer != nil {
		if ldb := relayer.GetLeaderboardDB(); ldb != nil {
			if db, ok := ldb.(*leaderboard.LeaderboardDB); ok {
				leaderboardHandler := leaderboard.NewHandler(db)
				http.Handle("/leaderboard", leaderboardHandler)

			} else {
				logrus.Warn("Leaderboard DB present but has unexpected type - /leaderboard not registered")
			}
		} else {
			logrus.Warn("Leaderboard database not available - /leaderboard endpoint not registered")
		}
	} else {
		logrus.Warn("Global relayer not initialized - leaderboard endpoint not registered")
	}

	captchaSecret := os.Getenv("CAPTCHA_SECRET")

	if xaiKey == "" {
		logrus.Warn("XAI_API_KEY not set — Grok endpoint will reject requests")
	}
	if grokCfg.Captcha.Enabled && captchaSecret == "" {
		logrus.Warn("CAPTCHA_SECRET not set but captcha is enabled — verification will fail")
	}

	grokHandler := grok.NewHandler(grokCfg, xaiKey, captchaSecret)
	http.Handle("/grok", grokHandler)
	logrus.WithField("captcha", grokCfg.Captcha.Enabled).Info("Grok AI chat endpoint registered at /grok")

	// --- Faucet endpoint ---
	// Reuses the same Ethereum client and chain ID from the global relayer.
	var faucetHandler *faucet.Handler
	rpcURL := os.Getenv("RPC_URL")
	if rpcURL != "" {
		faucetClient, err := ethclient.Dial(rpcURL)
		if err != nil {
			logrus.WithError(err).Warn("failed to create faucet eth client")
		} else {
			faucetChainID, err := faucetClient.ChainID(context.Background())
			if err != nil {
				logrus.WithError(err).Warn("failed to get chain ID for faucet")
			} else {
				faucetHandler = faucet.NewHandler(faucetClient, faucetChainID)
				http.Handle("/faucet", faucetHandler)
				logrus.Info("Faucet endpoint registered at /faucet")
			}
		}
	} else {
		logrus.Warn("RPC_URL not set — faucet endpoint not registered")
	}

	// --- Start HTTP server with graceful shutdown ---
	server := &http.Server{Addr: ":4001"}

	// Handle shutdown signals
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh

		logrus.WithField("signal", sig.String()).Info("Shutdown signal received")

		// Shutdown keeper first
		keeperCancel()

		// Close faucet handler database
		if faucetHandler != nil {
			if err := faucetHandler.Close(); err != nil {
				logrus.WithError(err).Error("Failed to close faucet database")
			}
		}

		// Shutdown HTTP server
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logrus.WithError(err).Error("HTTP server shutdown error")
		}
	}()

	logrus.Info("InstantExecution Relayer listening on :4001")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logrus.WithError(err).Fatal("HTTP server error")
	}

	logrus.Info("Server shutdown complete")
}
