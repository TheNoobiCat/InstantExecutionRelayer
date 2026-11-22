package sync

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/rs/zerolog"

	"github.com/thenoobicat/instantexecutionrelayer/keeper/config"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/metrics"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/orderbook"
)

// Event signatures (keccak256 of event signature string)
var (
	// PerpTrading events
	eventPositionOpened      = crypto.Keccak256Hash([]byte("PositionOpened(address,uint256,uint256,bool,uint256)"))
	eventPositionClosed      = crypto.Keccak256Hash([]byte("PositionClosed(address,uint256,int256,int256)"))
	eventPositionLiquidated  = crypto.Keccak256Hash([]byte("PositionLiquidated(address,uint256,address)"))
	eventPositionNetted      = crypto.Keccak256Hash([]byte("PositionNetted(address,uint256,uint256,bool)"))
	eventStopLossSet         = crypto.Keccak256Hash([]byte("StopLossSet(address,uint256)"))
	eventTakeProfitSet       = crypto.Keccak256Hash([]byte("TakeProfitSet(address,uint256)"))
	eventStopLossExecuted    = crypto.Keccak256Hash([]byte("StopLossExecuted(address,uint256,address)"))
	eventTakeProfitExecuted  = crypto.Keccak256Hash([]byte("TakeProfitExecuted(address,uint256,address)"))
	eventStopLossCancelled   = crypto.Keccak256Hash([]byte("StopLossCancelled(address)"))
	eventTakeProfitCancelled = crypto.Keccak256Hash([]byte("TakeProfitCancelled(address)"))

	// LimitOrderBook events
	eventLimitOrderCreated   = crypto.Keccak256Hash([]byte("LimitOrderCreated(bytes32,address,uint256,uint256,uint256)"))
	eventLimitOrderExecuted  = crypto.Keccak256Hash([]byte("LimitOrderExecuted(bytes32,address)"))
	eventLimitOrderCancelled = crypto.Keccak256Hash([]byte("LimitOrderCancelled(bytes32,address)"))
)

// Syncer synchronizes on-chain order state with the in-memory order books.
// It uses two strategies:
// 1. Event-driven: subscribe to contract events for real-time updates
// 2. Periodic full sync: scan all active positions/orders to catch missed events
type Syncer struct {
	ethClient     *ethclient.Client
	cfg           *config.Config
	logger        zerolog.Logger
	met           *metrics.Metrics
	books         map[string]*orderbook.MarketOrderBook
	marketConfigs map[string]*config.MarketConfig

	// Parsed ABIs for reading position/order data
	perpABI  abi.ABI
	limitABI abi.ABI

	// Optional Goldsky client for querying subgraph instead of RPC historical scan
	goldsky *GoldskyClient
}

// NewSyncer creates a new order syncer.
func NewSyncer(
	ethClient *ethclient.Client,
	cfg *config.Config,
	logger zerolog.Logger,
	met *metrics.Metrics,
	books map[string]*orderbook.MarketOrderBook,
) (*Syncer, error) {
	perpABI, err := abi.JSON(strings.NewReader(perpTradingABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse perp ABI: %w", err)
	}

	limitABI, err := abi.JSON(strings.NewReader(limitOrderBookABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse limit order ABI: %w", err)
	}

	marketConfigs := make(map[string]*config.MarketConfig)
	for i := range cfg.MonitoredMarkets {
		m := &cfg.MonitoredMarkets[i]
		marketConfigs[m.MarketSymbol] = m
	}

	// Create Goldsky client if URL is configured
	var goldskyClient *GoldskyClient
	if cfg.Sync.GoldskyURL != "" {
		goldskyClient = NewGoldskyClient(cfg.Sync.GoldskyURL)
		logger.Info().Str("goldsky_url", cfg.Sync.GoldskyURL).Msg("using Goldsky for order sync")
	}

	return &Syncer{
		ethClient:     ethClient,
		cfg:           cfg,
		logger:        logger.With().Str("component", "syncer").Logger(),
		met:           met,
		books:         books,
		marketConfigs: marketConfigs,
		perpABI:       perpABI,
		limitABI:      limitABI,
		goldsky:       goldskyClient,
	}, nil
}

// Run starts both event listening and periodic full sync.
// Blocks until context is cancelled.
func (s *Syncer) Run(ctx context.Context) error {
	// Do initial full sync on startup to load existing orders
	s.logger.Info().Msg("performing initial full sync of active positions and orders")
	if err := s.FullSync(ctx); err != nil {
		s.logger.Error().Err(err).Msg("initial full sync failed")
		// Continue anyway - events will catch up
	}

	// Start real-time event listener via WebSocket subscription for instant event detection.
	go s.listenEvents(ctx)
	if s.goldsky != nil {
		s.logger.Info().Msg("using Goldsky for bulk sync + WebSocket for real-time events")
	}

	// Periodic Goldsky poll as a lightweight safety net.
	// WebSocket should catch events instantly, but if subscription drops or
	// misses something, this ensures we reconcile every interval.
	// When Goldsky is configured this only queries the subgraph (no RPC spam).
	interval := time.Duration(s.cfg.Sync.FullSyncIntervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			start := time.Now()
			if err := s.FullSync(ctx); err != nil {
				s.logger.Error().Err(err).Msg("periodic sync failed")
				s.met.SyncErrors.Inc()
			} else {
				s.met.SyncDuration.Observe(time.Since(start).Seconds())
				s.met.LastSyncTime.SetToCurrentTime()

				for sym, book := range s.books {
					s.met.ActiveOrders.WithLabelValues(sym).Set(float64(book.Size()))
				}
			}
		}
	}
}

// FullSync reads all active positions and limit orders from the chain
// and rebuilds the order books.
func (s *Syncer) FullSync(ctx context.Context) error {
	for _, mCfg := range s.marketConfigs {
		if err := s.syncMarket(ctx, mCfg); err != nil {
			s.logger.Error().Err(err).Str("market", mCfg.MarketSymbol).Msg("market sync failed")
			continue
		}
	}
	return nil
}

// syncMarket syncs a single market's positions and limit orders.
func (s *Syncer) syncMarket(ctx context.Context, mCfg *config.MarketConfig) error {
	book := s.books[mCfg.MarketSymbol]
	if book == nil {
		return fmt.Errorf("no order book for market %s", mCfg.MarketSymbol)
	}

	tradingAddr := common.HexToAddress(mCfg.Trading)
	limitAddr := common.HexToAddress(mCfg.LimitOrderBook)

	// Scan for positions with SL/TP set by looking at recent events.
	// Skip when using Goldsky + WebSocket: historical scan hammers RPC with rate limit errors,
	// and real-time WebSocket subscription catches all new SL/TP events instantly anyway.
	if s.goldsky == nil {
		err := s.scanPositionEvents(ctx, mCfg, tradingAddr, book)
		if err != nil {
			s.logger.Warn().Err(err).Str("market", mCfg.MarketSymbol).Msg("position event scan failed")
		}
	}

	// Scan for active limit orders
	err := s.scanLimitOrderEvents(ctx, mCfg, limitAddr, book)
	if err != nil {
		s.logger.Warn().Err(err).Str("market", mCfg.MarketSymbol).Msg("limit order event scan failed")
	}

	s.logger.Info().
		Str("market", mCfg.MarketSymbol).
		Int("total_orders", book.Size()).
		Msg("market sync complete")

	return nil
}

// scanPositionEvents scans recent events to find positions with SL/TP set,
// then verifies each position on-chain. Uses chunked block range queries
// to stay within RPC provider limits (e.g., Alchemy free tier: 10-block max per eth_getLogs).
func (s *Syncer) scanPositionEvents(ctx context.Context, mCfg *config.MarketConfig, tradingAddr common.Address, book *orderbook.MarketOrderBook) error {
	latestBlock, err := s.ethClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("get block number: %w", err)
	}

	fromBlock := uint64(0)
	if latestBlock > s.cfg.Sync.StartupLookbackBlocks {
		fromBlock = latestBlock - s.cfg.Sync.StartupLookbackBlocks
	}

	// Query for PositionOpened, StopLossSet, TakeProfitSet events
	topics := [][]common.Hash{{
		eventPositionOpened,
		eventStopLossSet,
		eventTakeProfitSet,
	}}

	// Collect unique trader addresses from events using chunked queries
	traders := make(map[common.Address]bool)
	const chunkSize = uint64(5000) // Safe chunk size for most RPC providers

	for start := fromBlock; start <= latestBlock; start += chunkSize + 1 {
		end := start + chunkSize
		if end > latestBlock {
			end = latestBlock
		}

		query := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(start),
			ToBlock:   new(big.Int).SetUint64(end),
			Addresses: []common.Address{tradingAddr},
			Topics:    topics,
		}

		logs, err := s.ethClient.FilterLogs(ctx, query)
		if err != nil {
			s.logger.Warn().Err(err).
				Uint64("from", start).Uint64("to", end).
				Str("market", mCfg.MarketSymbol).
				Msg("position event chunk scan failed, skipping chunk")
			continue
		}

		for _, log := range logs {
			if len(log.Topics) >= 2 {
				trader := common.HexToAddress(log.Topics[1].Hex())
				traders[trader] = true
			}
		}
	}

	s.logger.Debug().Int("traders", len(traders)).Str("market", mCfg.MarketSymbol).Msg("found traders from events")

	// For each trader, read their current position on-chain
	for trader := range traders {
		s.syncTraderPosition(ctx, mCfg, tradingAddr, trader, book)
	}

	return nil
}

// calculateLiquidationPrice computes the approximate price at which a position
// becomes liquidatable. This is a conservative estimate (ignoring funding payments)
// used to place orders in the heap. The trigger engine verifies on-chain with
// canLiquidate() before executing.
//
// Uses 1% maintenance margin (100 bps) as a conservative default -- this is the
// most common tier (positions < 1% of pool, which is most retail positions at 50x max).
// If the actual maintenance margin is higher (larger positions), the position gets
// liquidated slightly earlier than this estimate, so on-chain canLiquidate() will
// catch it. Periodic re-sync every 30s also recalculates.
func calculateLiquidationPrice(collateralUsd, sizeUsd, entryPrice *big.Int, isLong bool) *big.Int {
	// maintenanceMargin = sizeUsd * 100 / 10000 = sizeUsd / 100 (1% of position size)
	maintenanceMargin := new(big.Int).Div(sizeUsd, big.NewInt(100))

	// buffer = collateralUsd - maintenanceMargin
	buffer := new(big.Int).Sub(collateralUsd, maintenanceMargin)
	if buffer.Sign() <= 0 {
		// Position is already below maintenance or at it -- use entry price as trigger
		// (it's basically immediately liquidatable)
		return new(big.Int).Set(entryPrice)
	}

	// delta = entryPrice * buffer / sizeUsd
	delta := new(big.Int).Mul(entryPrice, buffer)
	delta.Div(delta, sizeUsd)

	if isLong {
		// liqPrice = entryPrice - delta
		liqPrice := new(big.Int).Sub(entryPrice, delta)
		if liqPrice.Sign() <= 0 {
			return nil // Position can't be liquidated by price drop alone
		}
		return liqPrice
	}

	// Short: liqPrice = entryPrice + delta
	liqPrice := new(big.Int).Add(entryPrice, delta)
	return liqPrice
}

// syncTraderPosition reads a trader's position from the contract and adds SL/TP/Liquidation orders to the order book.
func (s *Syncer) syncTraderPosition(ctx context.Context, mCfg *config.MarketConfig, tradingAddr common.Address, trader common.Address, book *orderbook.MarketOrderBook) {
	calldata, err := s.perpABI.Pack("positions", trader)
	if err != nil {
		return
	}

	result, err := s.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &tradingAddr,
		Data: calldata,
	}, nil)
	if err != nil {
		return
	}

	outputs, err := s.perpABI.Unpack("positions", result)
	if err != nil || len(outputs) < 11 {
		return
	}

	// Parse position fields
	// positions returns: trader, collateral, collateralUsd, size, sizeUsd, entryPrice, lastFundingSnapshot, openTime, isLong, stopLossPrice, takeProfitPrice
	size, ok := outputs[3].(*big.Int)
	if !ok || size.Sign() == 0 {
		return // No active position
	}

	collateralUsd, _ := outputs[2].(*big.Int)
	sizeUsd, _ := outputs[4].(*big.Int)
	entryPrice, _ := outputs[5].(*big.Int)
	isLong, _ := outputs[8].(bool)
	stopLossPrice, _ := outputs[9].(*big.Int)
	takeProfitPrice, _ := outputs[10].(*big.Int)

	// Add stop loss order
	if stopLossPrice != nil && stopLossPrice.Sign() > 0 {
		slDirection := orderbook.TriggerBelow // Long SL: price drops below
		if !isLong {
			slDirection = orderbook.TriggerAbove // Short SL: price rises above
		}

		order := &orderbook.TriggerableOrder{
			ID:           fmt.Sprintf("sl-%s-%s", mCfg.MarketSymbol, trader.Hex()),
			Type:         orderbook.TriggerStopLoss,
			Direction:    slDirection,
			TriggerPrice: new(big.Int).Set(stopLossPrice),
			Trader:       trader,
			MarketSymbol: mCfg.MarketSymbol,
			TradingAddr:  tradingAddr,
			PythFeedID:   mCfg.PythPriceID,
			IsLong:       isLong,
			CreatedAt:    time.Now(),
		}
		book.AddOrder(order)
	}

	// Add take profit order
	if takeProfitPrice != nil && takeProfitPrice.Sign() > 0 {
		tpDirection := orderbook.TriggerAbove // Long TP: price rises above
		if !isLong {
			tpDirection = orderbook.TriggerBelow // Short TP: price drops below
		}

		order := &orderbook.TriggerableOrder{
			ID:           fmt.Sprintf("tp-%s-%s", mCfg.MarketSymbol, trader.Hex()),
			Type:         orderbook.TriggerTakeProfit,
			Direction:    tpDirection,
			TriggerPrice: new(big.Int).Set(takeProfitPrice),
			Trader:       trader,
			MarketSymbol: mCfg.MarketSymbol,
			TradingAddr:  tradingAddr,
			PythFeedID:   mCfg.PythPriceID,
			IsLong:       isLong,
			CreatedAt:    time.Now(),
		}
		book.AddOrder(order)
	}

	// Add liquidation trigger order
	// Calculate approximate liquidation price from position data.
	// The on-chain canLiquidate() check (via simulation) is the final arbiter;
	// this is just a conservative estimate to get the order into the heap.
	//
	// Liquidation condition: equity < maintenanceMargin
	//   equity = collateralUsd + pnl (ignoring funding for approximation)
	//   pnl(long) = sizeUsd * (currentPrice - entryPrice) / entryPrice
	//   pnl(short) = sizeUsd * (entryPrice - currentPrice) / entryPrice
	//
	// For longs, solve for: collateralUsd - sizeUsd*(entryPrice - liqPrice)/entryPrice = maintenanceMargin
	//   liqPrice = entryPrice * (1 - (collateralUsd - maintenanceMargin) / sizeUsd)
	//   liqPrice = entryPrice - entryPrice * (collateralUsd - maintenanceMargin) / sizeUsd
	//
	// For shorts, solve for: collateralUsd - sizeUsd*(liqPrice - entryPrice)/entryPrice = maintenanceMargin
	//   liqPrice = entryPrice + entryPrice * (collateralUsd - maintenanceMargin) / sizeUsd
	if collateralUsd != nil && sizeUsd != nil && entryPrice != nil &&
		collateralUsd.Sign() > 0 && sizeUsd.Sign() > 0 && entryPrice.Sign() > 0 {

		liqPrice := calculateLiquidationPrice(collateralUsd, sizeUsd, entryPrice, isLong)
		if liqPrice != nil && liqPrice.Sign() > 0 {
			// Long: liquidated when price drops below liqPrice -> TriggerBelow
			// Short: liquidated when price rises above liqPrice -> TriggerAbove
			liqDirection := orderbook.TriggerBelow
			if !isLong {
				liqDirection = orderbook.TriggerAbove
			}

			liqOrder := &orderbook.TriggerableOrder{
				ID:           fmt.Sprintf("liq-%s-%s", mCfg.MarketSymbol, trader.Hex()),
				Type:         orderbook.TriggerLiquidation,
				Direction:    liqDirection,
				TriggerPrice: liqPrice,
				Trader:       trader,
				MarketSymbol: mCfg.MarketSymbol,
				TradingAddr:  tradingAddr,
				PythFeedID:   mCfg.PythPriceID,
				IsLong:       isLong,
				CreatedAt:    time.Now(),
			}
			book.AddOrder(liqOrder)

			s.logger.Debug().
				Str("trader", trader.Hex()).
				Str("market", mCfg.MarketSymbol).
				Bool("isLong", isLong).
				Str("entry_price", entryPrice.String()).
				Str("liq_price", liqPrice.String()).
				Msg("added liquidation trigger")
		}
	}
}

// scanLimitOrderEvents scans for active limit orders.
// If Goldsky is configured, queries the subgraph. Otherwise, scans RPC events.
func (s *Syncer) scanLimitOrderEvents(ctx context.Context, mCfg *config.MarketConfig, limitAddr common.Address, book *orderbook.MarketOrderBook) error {
	// Use Goldsky if available (avoids RPC rate limits)
	if s.goldsky != nil {
		return s.scanLimitOrdersFromGoldsky(ctx, mCfg, limitAddr, book)
	}

	// Fallback to RPC event scanning
	return s.scanLimitOrdersFromRPC(ctx, mCfg, limitAddr, book)
}

// scanLimitOrdersFromGoldsky queries active orders from the subgraph.
func (s *Syncer) scanLimitOrdersFromGoldsky(ctx context.Context, mCfg *config.MarketConfig, limitAddr common.Address, book *orderbook.MarketOrderBook) error {
	orders, err := s.goldsky.FetchActiveLimitOrders(ctx, limitAddr)
	if err != nil {
		return fmt.Errorf("fetch from Goldsky: %w", err)
	}

	s.logger.Debug().Int("orders", len(orders)).Str("market", mCfg.MarketSymbol).Msg("fetched orders from Goldsky")

	// Add each order to the book
	for _, orderData := range orders {
		orderIDBytes, err := ParseOrderIDBytes(orderData.OrderID)
		if err != nil {
			s.logger.Warn().Err(err).Str("order_id", orderData.OrderID).Msg("invalid order ID from Goldsky")
			continue
		}

		// Verify order still active on-chain
		s.syncLimitOrder(ctx, mCfg, limitAddr, orderIDBytes, book)
	}

	return nil
}

// scanLimitOrdersFromRPC scans for LimitOrderCreated events via RPC (fallback when Goldsky unavailable).
// Uses chunked block range queries to stay within RPC provider limits.
func (s *Syncer) scanLimitOrdersFromRPC(ctx context.Context, mCfg *config.MarketConfig, limitAddr common.Address, book *orderbook.MarketOrderBook) error {
	latestBlock, err := s.ethClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("get block number: %w", err)
	}

	fromBlock := uint64(0)
	if latestBlock > s.cfg.Sync.StartupLookbackBlocks {
		fromBlock = latestBlock - s.cfg.Sync.StartupLookbackBlocks
	}

	const chunkSize = uint64(5000)

	// Collect unique order IDs from chunked queries
	orderIDs := make(map[[32]byte]bool)

	for start := fromBlock; start <= latestBlock; start += chunkSize + 1 {
		end := start + chunkSize
		if end > latestBlock {
			end = latestBlock
		}

		// Query for LimitOrderCreated events
		query := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(start),
			ToBlock:   new(big.Int).SetUint64(end),
			Addresses: []common.Address{limitAddr},
			Topics:    [][]common.Hash{{eventLimitOrderCreated}},
		}

		logs, err := s.ethClient.FilterLogs(ctx, query)
		if err != nil {
			s.logger.Warn().Err(err).Uint64("from", start).Uint64("to", end).
				Str("market", mCfg.MarketSymbol).Msg("limit order event chunk failed, skipping")
			continue
		}

		for _, log := range logs {
			if len(log.Topics) >= 2 {
				var orderID [32]byte
				copy(orderID[:], log.Topics[1].Bytes())
				orderIDs[orderID] = true
			}
		}

		// Also filter out cancelled/executed orders in same range
		cancelledQuery := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(start),
			ToBlock:   new(big.Int).SetUint64(end),
			Addresses: []common.Address{limitAddr},
			Topics:    [][]common.Hash{{eventLimitOrderExecuted, eventLimitOrderCancelled}},
		}

		cancelledLogs, err := s.ethClient.FilterLogs(ctx, cancelledQuery)
		if err == nil {
			for _, log := range cancelledLogs {
				if len(log.Topics) >= 2 {
					var orderID [32]byte
					copy(orderID[:], log.Topics[1].Bytes())
					delete(orderIDs, orderID)
				}
			}
		}
	}

	s.logger.Debug().Int("active_orders", len(orderIDs)).Str("market", mCfg.MarketSymbol).Msg("found limit orders from RPC events")

	// For each potentially active order, verify on-chain and add to book
	for orderID := range orderIDs {
		s.syncLimitOrder(ctx, mCfg, limitAddr, orderID, book)
	}

	return nil
}

// syncLimitOrder reads a limit order from the contract and adds it to the book if active.
func (s *Syncer) syncLimitOrder(ctx context.Context, mCfg *config.MarketConfig, limitAddr common.Address, orderID [32]byte, book *orderbook.MarketOrderBook) {
	calldata, err := s.limitABI.Pack("limitOrders", orderID)
	if err != nil {
		return
	}

	result, err := s.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &limitAddr,
		Data: calldata,
	}, nil)
	if err != nil {
		return
	}

	outputs, err := s.limitABI.Unpack("limitOrders", result)
	if err != nil || len(outputs) < 9 {
		return
	}

	// Parse: trader, collateral, size, isLong, triggerPrice, attachedStopLoss, attachedTakeProfit, expiryTime, createdTime
	trader, _ := outputs[0].(common.Address)
	if trader == (common.Address{}) {
		return // Order doesn't exist
	}

	isLong, _ := outputs[3].(bool)
	triggerPrice, _ := outputs[4].(*big.Int)
	expiryTime, _ := outputs[7].(*big.Int)

	if triggerPrice == nil || triggerPrice.Sign() == 0 {
		return
	}

	// Check if expired
	if expiryTime != nil && expiryTime.Int64() > 0 && time.Now().Unix() > expiryTime.Int64() {
		return
	}

	// Determine trigger direction:
	// Buy limit (isLong): execute when price <= trigger -> TriggerBelow
	// Sell limit (!isLong): execute when price >= trigger -> TriggerAbove
	direction := orderbook.TriggerBelow
	if !isLong {
		direction = orderbook.TriggerAbove
	}

	orderIDHex := "0x" + hex.EncodeToString(orderID[:])

	order := &orderbook.TriggerableOrder{
		ID:            fmt.Sprintf("limit-%s-%s", mCfg.MarketSymbol, orderIDHex),
		Type:          orderbook.TriggerLimitOrder,
		Direction:     direction,
		TriggerPrice:  new(big.Int).Set(triggerPrice),
		Trader:        trader,
		OrderID:       orderID,
		MarketSymbol:  mCfg.MarketSymbol,
		TradingAddr:   common.HexToAddress(mCfg.Trading),
		LimitBookAddr: common.HexToAddress(mCfg.LimitOrderBook),
		PythFeedID:    mCfg.PythPriceID,
		IsLong:        isLong,
		CreatedAt:     time.Now(),
	}
	book.AddOrder(order)
}

// listenEvents subscribes to real-time contract events to keep order books updated.
func (s *Syncer) listenEvents(ctx context.Context) {
	backoff := 5 * time.Second
	maxBackoff := 60 * time.Second

	for {
		err := s.subscribeAndProcess(ctx)
		if ctx.Err() != nil {
			return
		}

		errStr := ""
		if err != nil {
			errStr = err.Error()
		}

		// If the RPC doesn't support subscriptions at all, stop retrying
		if strings.Contains(errStr, "notifications not supported") ||
			strings.Contains(errStr, "not available") {
			s.logger.Error().Err(err).Msg("RPC does not support WebSocket subscriptions; falling back to polling only")
			return
		}

		s.logger.Warn().Err(err).Dur("retry_in", backoff).Msg("event subscription lost, reconnecting")
		time.Sleep(backoff)

		// Exponential backoff capped at maxBackoff, reset on successful reconnect
		backoff = backoff * 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (s *Syncer) subscribeAndProcess(ctx context.Context) error {
	// Build filter for all monitored contracts
	var addresses []common.Address
	for _, mCfg := range s.marketConfigs {
		addresses = append(addresses, common.HexToAddress(mCfg.Trading))
		addresses = append(addresses, common.HexToAddress(mCfg.LimitOrderBook))
	}

	// Subscribe to all relevant events
	query := ethereum.FilterQuery{
		Addresses: addresses,
		Topics: [][]common.Hash{{
			eventPositionOpened,
			eventPositionClosed,
			eventPositionLiquidated,
			eventPositionNetted,
			eventStopLossSet,
			eventTakeProfitSet,
			eventStopLossExecuted,
			eventTakeProfitExecuted,
			eventStopLossCancelled,
			eventTakeProfitCancelled,
			eventLimitOrderCreated,
			eventLimitOrderExecuted,
			eventLimitOrderCancelled,
		}},
	}

	logCh := make(chan types.Log, 256)
	sub, err := s.ethClient.SubscribeFilterLogs(ctx, query, logCh)
	if err != nil {
		return fmt.Errorf("subscribe filter logs: %w", err)
	}
	defer sub.Unsubscribe()

	s.logger.Info().Int("addresses", len(addresses)).Msg("subscribed to contract events")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("subscription error: %w", err)
		case log := <-logCh:
			s.processEvent(ctx, log)
		}
	}
}

// processEvent handles a single contract event log.
func (s *Syncer) processEvent(ctx context.Context, log types.Log) {
	if len(log.Topics) == 0 {
		return
	}

	eventSig := log.Topics[0]

	// Find which market this event belongs to
	var mCfg *config.MarketConfig
	logAddr := log.Address
	for _, mc := range s.marketConfigs {
		if common.HexToAddress(mc.Trading) == logAddr || common.HexToAddress(mc.LimitOrderBook) == logAddr {
			mCfg = mc
			break
		}
	}
	if mCfg == nil {
		return
	}

	book := s.books[mCfg.MarketSymbol]
	if book == nil {
		return
	}

	tradingAddr := common.HexToAddress(mCfg.Trading)
	limitAddr := common.HexToAddress(mCfg.LimitOrderBook)

	switch eventSig {
	case eventStopLossSet, eventTakeProfitSet, eventPositionOpened, eventPositionNetted:
		// Re-sync the trader's position (might have new SL/TP or changed size from netting)
		if len(log.Topics) >= 2 {
			trader := common.HexToAddress(log.Topics[1].Hex())
			s.syncTraderPosition(ctx, mCfg, tradingAddr, trader, book)
		}

	case eventPositionClosed, eventPositionLiquidated, eventStopLossExecuted, eventTakeProfitExecuted:
		// Position closed - remove all orders for this trader (SL, TP, and liquidation)
		if len(log.Topics) >= 2 {
			trader := common.HexToAddress(log.Topics[1].Hex())
			book.RemoveOrder(fmt.Sprintf("sl-%s-%s", mCfg.MarketSymbol, trader.Hex()))
			book.RemoveOrder(fmt.Sprintf("tp-%s-%s", mCfg.MarketSymbol, trader.Hex()))
			book.RemoveOrder(fmt.Sprintf("liq-%s-%s", mCfg.MarketSymbol, trader.Hex()))
		}

	case eventStopLossCancelled:
		if len(log.Topics) >= 2 {
			trader := common.HexToAddress(log.Topics[1].Hex())
			book.RemoveOrder(fmt.Sprintf("sl-%s-%s", mCfg.MarketSymbol, trader.Hex()))
		}

	case eventTakeProfitCancelled:
		if len(log.Topics) >= 2 {
			trader := common.HexToAddress(log.Topics[1].Hex())
			book.RemoveOrder(fmt.Sprintf("tp-%s-%s", mCfg.MarketSymbol, trader.Hex()))
		}

	case eventLimitOrderCreated:
		// Sync the new limit order
		if len(log.Topics) >= 2 {
			var orderID [32]byte
			copy(orderID[:], log.Topics[1].Bytes())
			s.syncLimitOrder(ctx, mCfg, limitAddr, orderID, book)
		}

	case eventLimitOrderExecuted, eventLimitOrderCancelled:
		// Remove the limit order
		if len(log.Topics) >= 2 {
			var orderID [32]byte
			copy(orderID[:], log.Topics[1].Bytes())
			orderIDHex := "0x" + hex.EncodeToString(orderID[:])
			book.RemoveOrder(fmt.Sprintf("limit-%s-%s", mCfg.MarketSymbol, orderIDHex))
		}
	}

	// Log event with descriptive name and context
	eventName := "Unknown"
	logFields := s.logger.Debug()

	switch eventSig {
	case eventPositionOpened:
		eventName = "PositionOpened"
	case eventPositionClosed:
		eventName = "PositionClosed"
	case eventPositionLiquidated:
		eventName = "PositionLiquidated"
	case eventPositionNetted:
		eventName = "PositionNetted"
	case eventStopLossSet:
		eventName = "StopLossSet"
	case eventTakeProfitSet:
		eventName = "TakeProfitSet"
	case eventStopLossExecuted:
		eventName = "StopLossExecuted"
	case eventTakeProfitExecuted:
		eventName = "TakeProfitExecuted"
	case eventStopLossCancelled:
		eventName = "StopLossCancelled"
	case eventTakeProfitCancelled:
		eventName = "TakeProfitCancelled"
	case eventLimitOrderCreated:
		eventName = "LimitOrderCreated"
		if len(log.Topics) >= 2 {
			logFields = logFields.Str("order_id", log.Topics[1].Hex()[:10])
		}
	case eventLimitOrderExecuted:
		eventName = "LimitOrderExecuted"
		if len(log.Topics) >= 2 {
			logFields = logFields.Str("order_id", log.Topics[1].Hex()[:10])
		}
	case eventLimitOrderCancelled:
		eventName = "LimitOrderCancelled"
		if len(log.Topics) >= 2 {
			logFields = logFields.Str("order_id", log.Topics[1].Hex()[:10])
		}
	}

	// Add trader address for all trader-indexed events
	if len(log.Topics) >= 2 && eventSig != eventLimitOrderCreated && eventSig != eventLimitOrderExecuted && eventSig != eventLimitOrderCancelled {
		logFields = logFields.Str("trader", log.Topics[1].Hex()[:10])
	}

	logFields.
		Str("event", eventName).
		Str("market", mCfg.MarketSymbol).
		Str("tx", log.TxHash.Hex()[:10]).
		Msg("processed event")
}

// ABI definitions needed by the syncer (same as trigger engine)
const perpTradingABIJSON = `[
	{
		"name": "positions",
		"type": "function",
		"stateMutability": "view",
		"inputs": [{"name": "trader", "type": "address"}],
		"outputs": [
			{"name": "trader", "type": "address"},
			{"name": "collateral", "type": "uint256"},
			{"name": "collateralUsd", "type": "uint256"},
			{"name": "size", "type": "uint256"},
			{"name": "sizeUsd", "type": "uint256"},
			{"name": "entryPrice", "type": "uint256"},
			{"name": "lastFundingSnapshot", "type": "int256"},
			{"name": "openTime", "type": "uint256"},
			{"name": "isLong", "type": "bool"},
			{"name": "stopLossPrice", "type": "uint256"},
			{"name": "takeProfitPrice", "type": "uint256"}
		]
	}
]`

const limitOrderBookABIJSON = `[
	{
		"name": "limitOrders",
		"type": "function",
		"stateMutability": "view",
		"inputs": [{"name": "orderId", "type": "bytes32"}],
		"outputs": [
			{"name": "trader", "type": "address"},
			{"name": "collateral", "type": "uint256"},
			{"name": "size", "type": "uint256"},
			{"name": "isLong", "type": "bool"},
			{"name": "triggerPrice", "type": "uint256"},
			{"name": "attachedStopLoss", "type": "uint256"},
			{"name": "attachedTakeProfit", "type": "uint256"},
			{"name": "expiryTime", "type": "uint256"},
			{"name": "createdTime", "type": "uint256"}
		]
	}
]`
