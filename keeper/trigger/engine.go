package trigger

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/rs/zerolog"

	"github.com/thenoobicat/instantexecutionrelayer/keeper/config"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/metrics"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/orderbook"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/pyth"
)

// ABIs for the contract functions we need to call.
// These match the exact function signatures in BrokerFi's PerpTrading.sol and LimitOrderBook.sol

const perpTradingABIJSON = `[
	{
		"name": "executeStopLoss",
		"type": "function",
		"stateMutability": "payable",
		"inputs": [
			{"name": "trader", "type": "address"},
			{"name": "priceUpdateData", "type": "bytes[]"}
		],
		"outputs": []
	},
	{
		"name": "executeTakeProfit",
		"type": "function",
		"stateMutability": "payable",
		"inputs": [
			{"name": "trader", "type": "address"},
			{"name": "priceUpdateData", "type": "bytes[]"}
		],
		"outputs": []
	},
	{
		"name": "liquidatePosition",
		"type": "function",
		"stateMutability": "nonpayable",
		"inputs": [
			{"name": "trader", "type": "address"},
			{"name": "priceUpdateData", "type": "bytes[]"}
		],
		"outputs": []
	},
	{
		"name": "canExecuteStopLoss",
		"type": "function",
		"stateMutability": "view",
		"inputs": [{"name": "trader", "type": "address"}],
		"outputs": [
			{"name": "canExecute", "type": "bool"},
			{"name": "currentPrice", "type": "uint256"}
		]
	},
	{
		"name": "canExecuteTakeProfit",
		"type": "function",
		"stateMutability": "view",
		"inputs": [{"name": "trader", "type": "address"}],
		"outputs": [
			{"name": "canExecute", "type": "bool"},
			{"name": "currentPrice", "type": "uint256"}
		]
	},
	{
		"name": "canLiquidate",
		"type": "function",
		"stateMutability": "view",
		"inputs": [{"name": "trader", "type": "address"}],
		"outputs": [{"name": "", "type": "bool"}]
	},
	{
		"name": "getCurrentPrice",
		"type": "function",
		"stateMutability": "view",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}]
	},
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
		"name": "executeLimitOrder",
		"type": "function",
		"stateMutability": "payable",
		"inputs": [
			{"name": "orderId", "type": "bytes32"},
			{"name": "priceUpdateData", "type": "bytes[]"}
		],
		"outputs": []
	},
	{
		"name": "canExecuteOrder",
		"type": "function",
		"stateMutability": "view",
		"inputs": [{"name": "orderId", "type": "bytes32"}],
		"outputs": [{"name": "", "type": "bool"}]
	},
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
	},
	{
		"name": "nextOrderId",
		"type": "function",
		"stateMutability": "view",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}]
	}
]`

// Pyth ABI for getUpdateFee
const pythABIJSON = `[
	{
		"name": "getUpdateFee",
		"type": "function",
		"stateMutability": "view",
		"inputs": [{"name": "updateData", "type": "bytes[]"}],
		"outputs": [{"name": "feeAmount", "type": "uint256"}]
	}
]`

// Engine is the core trigger detection and simulation engine.
// It receives price updates from the Pyth SSE stream, checks them against
// the order heaps, simulates candidate transactions, and dispatches them
// to the executor.
type Engine struct {
	ethClient  *ethclient.Client
	pythClient *pyth.Client
	cfg        *config.Config
	logger     zerolog.Logger
	met        *metrics.Metrics

	// Keeper wallet address for simulation (From field in eth_call/EstimateGas)
	keeperAddr common.Address

	// Per-market order books
	books         map[string]*orderbook.MarketOrderBook // marketSymbol -> book
	marketConfigs map[string]*config.MarketConfig       // marketSymbol -> config

	// Parsed ABIs
	perpABI  abi.ABI
	limitABI abi.ABI
	pythABI  abi.ABI

	// Feed ID -> market symbol mapping
	feedToMarket map[string]string

	// Executor callback
	onTrigger TriggerCallback

	// Track in-flight orders to avoid double-submission
	inFlight sync.Map // orderID -> time.Time (when submitted)
}

// TriggerResult contains the simulated transaction data ready for submission.
type TriggerResult struct {
	Order       *orderbook.TriggerableOrder
	To          common.Address
	Calldata    []byte
	Value       *big.Int // msg.value for Pyth fee
	GasEstimate uint64
	SimulatedAt time.Time
}

// TriggerCallback is called when an order is confirmed triggerable via simulation.
type TriggerCallback func(result *TriggerResult)

// NewEngine creates a new trigger engine.
func NewEngine(
	ethClient *ethclient.Client,
	pythClient *pyth.Client,
	keeperAddr common.Address,
	cfg *config.Config,
	logger zerolog.Logger,
	met *metrics.Metrics,
	onTrigger TriggerCallback,
) (*Engine, error) {
	perpABI, err := abi.JSON(strings.NewReader(perpTradingABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse perp ABI: %w", err)
	}

	limitABI, err := abi.JSON(strings.NewReader(limitOrderBookABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse limit order ABI: %w", err)
	}

	pythABIParsed, err := abi.JSON(strings.NewReader(pythABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse pyth ABI: %w", err)
	}

	books := make(map[string]*orderbook.MarketOrderBook)
	marketConfigs := make(map[string]*config.MarketConfig)
	feedToMarket := make(map[string]string)

	for i := range cfg.MonitoredMarkets {
		m := &cfg.MonitoredMarkets[i]
		books[m.MarketSymbol] = orderbook.NewMarketOrderBook(m.MarketSymbol)
		marketConfigs[m.MarketSymbol] = m
		feedID := "0x" + strings.ToLower(strings.TrimPrefix(m.PythPriceID, "0x"))
		feedToMarket[feedID] = m.MarketSymbol
	}

	return &Engine{
		ethClient:     ethClient,
		pythClient:    pythClient,
		keeperAddr:    keeperAddr,
		cfg:           cfg,
		logger:        logger.With().Str("component", "trigger-engine").Logger(),
		met:           met,
		books:         books,
		marketConfigs: marketConfigs,
		perpABI:       perpABI,
		limitABI:      limitABI,
		pythABI:       pythABIParsed,
		feedToMarket:  feedToMarket,
		onTrigger:     onTrigger,
	}, nil
}

// GetOrderBook returns the order book for a given market.
func (e *Engine) GetOrderBook(marketSymbol string) *orderbook.MarketOrderBook {
	return e.books[marketSymbol]
}

// GetOrderBooks returns all order books keyed by market symbol.
// Used to share the same book instances with the syncer.
func (e *Engine) GetOrderBooks() map[string]*orderbook.MarketOrderBook {
	return e.books
}

// OnPriceUpdate is the callback handler for Pyth SSE price updates.
// This is the HOT PATH - called on every price tick from the stream.
// It must be as fast as possible: O(1) market lookup + O(log n) heap peek/pop.
func (e *Engine) OnPriceUpdate(update pyth.PriceUpdate) {
	startTime := time.Now()

	// Map feed ID to market
	marketSymbol, ok := e.feedToMarket[update.FeedID]
	if !ok {
		return // Not a monitored feed
	}

	book, ok := e.books[marketSymbol]
	if !ok {
		return
	}

	// Convert the streamed price to the contract's 1e18 format
	// Contract prices are in 18 decimals. Pyth sends price * 10^expo.
	// We need: price * 10^(18 + expo)
	// Example: price=6140993501000, expo=-8 -> price * 10^10 = 61409935010000000000000 (1e18 format)
	currentPrice := pythPriceToContract(update.Price, update.Expo)

	if currentPrice == nil || currentPrice.Sign() <= 0 {
		return
	}

	// Get all triggered orders from the heaps - O(k * log n) where k = number triggered
	triggered := book.GetTriggeredOrders(currentPrice)

	if len(triggered) == 0 {
		return
	}

	e.logger.Info().
		Str("market", marketSymbol).
		Int("triggered", len(triggered)).
		Str("price", currentPrice.String()).
		Float64("price_float", update.PriceFloat).
		Msg("orders triggered")

	e.met.OrdersTriggered.WithLabelValues(marketSymbol).Add(float64(len(triggered)))

	// Process each triggered order: simulate then dispatch
	for _, order := range triggered {
		// Check if already in-flight
		if _, loaded := e.inFlight.Load(order.ID); loaded {
			e.logger.Debug().Str("order", order.ID).Msg("order already in-flight, skipping")
			// Re-add to book if not too many attempts
			if order.Attempts < e.cfg.Execution.MaxRetries {
				book.AddOrder(order)
			}
			continue
		}

		// Check retry backoff
		if !order.LastAttempt.IsZero() &&
			time.Since(order.LastAttempt) < time.Duration(e.cfg.Execution.RetryBackoffSec)*time.Second {
			book.AddOrder(order) // Put it back
			continue
		}

		// Simulate in a goroutine to not block the price update handler
		go e.simulateAndDispatch(order, update)
	}

	// Record end-to-end trigger detection latency
	triggerLatency := time.Since(startTime)
	e.met.TriggerLatency.Observe(triggerLatency.Seconds())
}

// simulateAndDispatch simulates the transaction and, if successful, dispatches to executor.
func (e *Engine) simulateAndDispatch(order *orderbook.TriggerableOrder, priceUpdate pyth.PriceUpdate) {
	startTime := time.Now()

	order.Attempts++
	order.LastAttempt = time.Now()
	e.inFlight.Store(order.ID, time.Now())
	defer e.inFlight.Delete(order.ID)

	mCfg := e.marketConfigs[order.MarketSymbol]

	// Get fresh binary update data for the contract call
	updateBytes, err := e.pythClient.GetLatestBinaryUpdateData(
		order.PythFeedID,
		int64(e.cfg.Execution.PythUpdateMaxAgeSec),
	)
	if err != nil {
		e.logger.Error().Err(err).Str("order", order.ID).Msg("failed to get Pyth update data")
		e.met.SimulationResults.WithLabelValues(order.MarketSymbol, "pyth_error").Inc()
		e.requeueOrder(order)
		return
	}

	// Build the calldata and determine target contract
	var (
		calldata []byte
		target   common.Address
		value    *big.Int
	)

	// Build priceUpdateData as bytes[] with single element
	priceUpdateData := [][]byte{updateBytes}

	switch order.Type {
	case orderbook.TriggerStopLoss:
		calldata, err = e.perpABI.Pack("executeStopLoss", order.Trader, priceUpdateData)
		target = common.HexToAddress(mCfg.Trading)

	case orderbook.TriggerTakeProfit:
		calldata, err = e.perpABI.Pack("executeTakeProfit", order.Trader, priceUpdateData)
		target = common.HexToAddress(mCfg.Trading)

	case orderbook.TriggerLimitOrder:
		calldata, err = e.limitABI.Pack("executeLimitOrder", order.OrderID, priceUpdateData)
		target = common.HexToAddress(mCfg.LimitOrderBook)

	case orderbook.TriggerLiquidation:
		calldata, err = e.perpABI.Pack("liquidatePosition", order.Trader, priceUpdateData)
		target = common.HexToAddress(mCfg.Trading)
	}

	if err != nil {
		e.logger.Error().Err(err).Str("order", order.ID).Str("type", order.Type.String()).Msg("failed to pack calldata")
		e.met.SimulationResults.WithLabelValues(order.MarketSymbol, "abi_error").Inc()
		return
	}

	// Calculate Pyth update fee via eth_call to Pyth contract
	value, err = e.getPythUpdateFee(priceUpdateData)
	if err != nil {
		e.logger.Warn().Err(err).Msg("failed to get Pyth fee, using default 1 wei")
		value = big.NewInt(1) // Default: 1 wei per update on most chains
	}

	// For liquidatePosition: function is nonpayable, so msg.value must be 0.
	// The contract pays the Pyth update fee from its own ETH balance.
	if order.Type == orderbook.TriggerLiquidation {
		value = big.NewInt(0)
	}

	// Simulate via eth_call
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(e.cfg.Execution.SimulationTimeoutMs)*time.Millisecond)
	defer cancel()

	gasEstimate, err := e.simulate(ctx, target, calldata, value)
	if err != nil {
		errStr := err.Error()
		revertReason := "unknown"
		switch {
		case strings.Contains(errStr, "StopLossNotTriggered") || strings.Contains(errStr, "TakeProfitNotTriggered") ||
			strings.Contains(errStr, "TriggerNotMet") || strings.Contains(errStr, "PositionNotLiquidatable"):
			revertReason = "not_triggered"
			e.logger.Debug().Str("order", order.ID).Str("type", order.Type.String()).Msg("simulation: trigger condition not met")
		case strings.Contains(errStr, "NoPositionToClose") || strings.Contains(errStr, "OrderNotFound"):
			revertReason = "already_executed"
			e.logger.Info().Str("order", order.ID).Msg("order already executed or position closed")
			e.met.SimulationResults.WithLabelValues(order.MarketSymbol, revertReason).Inc()
			return
		case strings.Contains(errStr, "OrderExpired"):
			revertReason = "expired"
			e.logger.Info().Str("order", order.ID).Msg("limit order expired")
			e.met.SimulationResults.WithLabelValues(order.MarketSymbol, revertReason).Inc()
			return
		case strings.Contains(errStr, "stale") || strings.Contains(errStr, "PriceFeedNotFound") || strings.Contains(errStr, "PriceFeedNotSet"):
			revertReason = "stale_price"
			e.logger.Warn().Str("order", order.ID).Msg("simulation: stale or missing price feed")
		case strings.Contains(errStr, "InsufficientCollateralForFees") || strings.Contains(errStr, "CollateralBelowMinimum"):
			revertReason = "insufficient_collateral"
			e.logger.Info().Str("order", order.ID).Msg("order collateral insufficient for fees")
			e.met.SimulationResults.WithLabelValues(order.MarketSymbol, revertReason).Inc()
			return
		case strings.Contains(errStr, "InsufficientFee"):
			revertReason = "pyth_fee"
			e.logger.Warn().Str("order", order.ID).Msg("simulation: Pyth update fee insufficient")
		case strings.Contains(errStr, "opposite directions"):
			revertReason = "opposing_position"
			e.logger.Info().Str("order", order.ID).Msg("trader has opposing position, cannot execute limit order")
			e.met.SimulationResults.WithLabelValues(order.MarketSymbol, revertReason).Inc()
			return
		default:
			e.logger.Warn().Str("order", order.ID).Str("type", order.Type.String()).
				Str("revert_reason", errStr).Msg("simulation reverted")
		}
		e.met.SimulationResults.WithLabelValues(order.MarketSymbol, revertReason).Inc()
		e.requeueOrder(order)
		return
	}

	e.met.SimulationResults.WithLabelValues(order.MarketSymbol, "success").Inc()

	// Apply gas limit buffer
	bufferedGas := uint64(float64(gasEstimate) * e.cfg.Gas.GasLimitBuffer)

	// Check gas cost against max
	if maxGas := mCfg.GetMaxGasWei(); maxGas != nil {
		// Rough gas cost check: gasEstimate * current gas price
		// Detailed check happens in executor
		e.logger.Debug().Uint64("gas_estimate", bufferedGas).Str("max_gas", maxGas.String()).Msg("gas check")
	}

	simLatency := time.Since(startTime)
	e.met.SimulationLatency.Observe(simLatency.Seconds())

	e.logger.Info().
		Str("order", order.ID).
		Str("type", order.Type.String()).
		Uint64("gas", bufferedGas).
		Dur("sim_latency", simLatency).
		Msg("simulation succeeded, dispatching to executor")

	// Dispatch to executor
	if e.onTrigger != nil {
		e.onTrigger(&TriggerResult{
			Order:       order,
			To:          target,
			Calldata:    calldata,
			Value:       value,
			GasEstimate: bufferedGas,
			SimulatedAt: time.Now(),
		})
	}
}

// simulate performs eth_call to check for revert reason, then EstimateGas for gas.
func (e *Engine) simulate(ctx context.Context, to common.Address, calldata []byte, value *big.Int) (uint64, error) {
	msg := ethereum.CallMsg{
		From:  e.keeperAddr,
		To:    &to,
		Data:  calldata,
		Value: value,
		Gas:   0,
	}

	// First do eth_call to get revert data if it fails
	_, err := e.ethClient.CallContract(ctx, msg, nil)
	if err != nil {
		revertReason := extractRevertReason(err)
		if revertReason != "" {
			return 0, fmt.Errorf("simulation failed: %s", revertReason)
		}
		return 0, fmt.Errorf("simulation failed: %w", err)
	}

	// eth_call succeeded, now get gas estimate
	gasEstimate, err := e.ethClient.EstimateGas(ctx, msg)
	if err != nil {
		return 0, fmt.Errorf("gas estimation failed: %w", err)
	}

	return gasEstimate, nil
}

// extractRevertReason tries to decode a human-readable revert reason from an RPC error.
// It checks three sources in order:
// 1. rpc.DataError interface (Alchemy/Infura put revert data in ErrorData())
// 2. Hex data embedded in the error string ("execution reverted: 0x...")
// 3. Known error names in plain text
func extractRevertReason(err error) string {
	if err == nil {
		return ""
	}

	// --- Source 1: rpc.DataError interface ---
	// Alchemy and most providers return a JSON-RPC error where the revert data
	// is in the "data" field, NOT in the error message. go-ethereum exposes this
	// via the rpc.DataError interface.
	var dataErr rpc.DataError
	if errors.As(err, &dataErr) {
		errData := dataErr.ErrorData()
		if errData != nil {
			var revertBytes []byte
			switch v := errData.(type) {
			case string:
				// Most common: hex string like "0x665bf3e6" or without 0x prefix
				cleaned := strings.TrimPrefix(v, "0x")
				if len(cleaned) > 0 {
					decoded, decErr := hex.DecodeString(cleaned)
					if decErr == nil {
						revertBytes = decoded
					}
				}
			case []byte:
				revertBytes = v
			}
			if len(revertBytes) > 0 {
				return decodeRevertData(revertBytes)
			}
			// ErrorData was present but not decodable - return it as string for debugging
			return fmt.Sprintf("raw_error_data: %v", errData)
		}
	}

	errStr := err.Error()

	// --- Source 2: Hex data in error message ---
	// Some RPC providers include the revert data in the error message
	// Format: "execution reverted: 0x..." or embedded hex data
	if idx := strings.Index(errStr, "0x"); idx >= 0 {
		hexData := errStr[idx:]
		// Trim any trailing non-hex chars
		end := len(hexData)
		for i := 2; i < len(hexData); i++ {
			c := hexData[i]
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				end = i
				break
			}
		}
		hexData = hexData[:end]

		if len(hexData) >= 10 { // "0x" + at least 4 bytes (8 hex chars)
			decoded, decErr := hex.DecodeString(hexData[2:])
			if decErr == nil {
				return decodeRevertData(decoded)
			}
		}
	}

	// --- Source 3: Known error names as plain text ---
	knownErrors := []string{
		"OrderNotFound", "OrderExpired", "TriggerNotMet",
		"OnlyLimitOrderBook", "PriceFeedNotSet",
		"InsufficientCollateralForFees", "CollateralBelowMinimum",
		"PositionSizeBelowMinimum", "InvalidPrice", "Price overflow",
		"Cannot merge opposite directions",
		"StopLossNotTriggered", "TakeProfitNotTriggered",
		"NoPositionToClose", "InsufficientFee",
	}
	for _, known := range knownErrors {
		if strings.Contains(errStr, known) {
			return known
		}
	}

	return ""
}

// decodeRevertData decodes raw revert bytes into a human-readable string.
func decodeRevertData(data []byte) string {
	if len(data) < 4 {
		return hex.EncodeToString(data)
	}

	selector := hex.EncodeToString(data[:4])

	// Error(string) selector: 08c379a0
	if selector == "08c379a0" && len(data) >= 68 {
		// ABI-decode the string
		strABI, _ := abi.JSON(strings.NewReader(`[{"name":"Error","type":"function","inputs":[{"name":"reason","type":"string"}]}]`))
		vals, err := strABI.Methods["Error"].Inputs.Unpack(data[4:])
		if err == nil && len(vals) > 0 {
			if reason, ok := vals[0].(string); ok {
				return reason
			}
		}
	}

	// Panic(uint256) selector: 4e487b71
	if selector == "4e487b71" && len(data) >= 36 {
		code := new(big.Int).SetBytes(data[4:36])
		panicCodes := map[int64]string{
			0x01: "assertion failure",
			0x11: "arithmetic overflow/underflow",
			0x12: "division by zero",
			0x21: "invalid enum value",
			0x22: "storage encoding error",
			0x31: "pop on empty array",
			0x32: "array out of bounds",
			0x41: "out of memory",
			0x51: "zero-initialized function pointer",
		}
		if desc, ok := panicCodes[code.Int64()]; ok {
			return fmt.Sprintf("Panic(%s)", desc)
		}
		return fmt.Sprintf("Panic(0x%x)", code)
	}

	// Map known custom error selectors from BrokerFi contracts
	customErrors := map[string]string{
		"d36d8965": "OrderNotFound()",
		"c56873ba": "OrderExpired()",
		"665bf3e6": "TriggerNotMet()",
		"57270435": "OnlyLimitOrderBook()",
		"7e68a045": "PriceFeedNotSet()",
		"455fd494": "InsufficientCollateralForFees()",
		"735608ca": "CollateralBelowMinimum()",
		"c4933979": "PositionSizeBelowMinimum()",
		"025dbdd4": "InsufficientFee()",
		"04f77d69": "NoPositionToClose()",
		"dfa9fa30": "StopLossNotTriggered()",
		"554b9922": "TakeProfitNotTriggered()",
		"7fd13972": "InvalidLeverage()",
		"0a4f5a07": "LeverageTooHigh()",
		"641f87fc": "LeverageTooLow()",
		"a27ef906": "CollateralRequired()",
		"bdee9b4d": "SizeTooSmall()",
		"20f3ba41": "SizeRequired()",
		"3ee5aeb5": "ReentrancyGuardReentrantCall()",
		"d93c0665": "EnforcedPause()",
		"5274afe7": "SafeERC20FailedOperation(address)",
		"cc86e376": "CollateralLeverageOverflow()",
		"a98699de": "InvalidStopLossPrice()",
		"93e7c2c3": "InvalidTakeProfitPrice()",
		"8199f5f3": "SlippageExceeded()",
		"4cb429ac": "NoStopLossSet()",
		"37041ad5": "NoTakeProfitSet()",
		"6415f959": "PositionNotLiquidatable()",
		"cd4e6167": "FeeTooHigh()",
		"f6412b5a": "NotOrderOwner()",
		"d36c8500": "InvalidExpiry()",
		"30404495": "InvalidAttachedStopLoss()",
		"ff9f0d85": "InvalidAttachedTakeProfit()",
	}
	if name, ok := customErrors[selector]; ok {
		return name
	}

	return fmt.Sprintf("custom error 0x%s (data: %d bytes)", selector, len(data))
}

// getPythUpdateFee calls pyth.getUpdateFee(bytes[]) to determine the required msg.value.
func (e *Engine) getPythUpdateFee(updateData [][]byte) (*big.Int, error) {
	pythAddr := common.HexToAddress(e.cfg.Contracts.PythAddress)
	calldata, err := e.pythABI.Pack("getUpdateFee", updateData)
	if err != nil {
		return nil, fmt.Errorf("pack getUpdateFee: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := e.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &pythAddr,
		Data: calldata,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("call getUpdateFee: %w", err)
	}

	outputs, err := e.pythABI.Unpack("getUpdateFee", result)
	if err != nil {
		return nil, fmt.Errorf("unpack getUpdateFee: %w", err)
	}

	if len(outputs) == 0 {
		return big.NewInt(1), nil
	}

	fee, ok := outputs[0].(*big.Int)
	if !ok {
		return big.NewInt(1), nil
	}

	return fee, nil
}

// requeueOrder puts a failed order back into the book for retry.
func (e *Engine) requeueOrder(order *orderbook.TriggerableOrder) {
	if order.Attempts >= e.cfg.Execution.MaxRetries {
		e.logger.Warn().Str("order", order.ID).Int("attempts", order.Attempts).Msg("max retries exceeded, dropping order")
		e.met.OrdersDropped.WithLabelValues(order.MarketSymbol).Inc()
		return
	}

	book := e.books[order.MarketSymbol]
	if book != nil {
		book.AddOrder(order)
	}
}

// pythPriceToContract converts Pyth price (int64 * 10^expo) to contract format (uint256 * 10^18).
// This matches the normalization done in PythUtils.sol:
//
//	if expo >= 0: price * 10^(18+expo) -- but this is unusual
//	if |expo| <= 18: price * 10^(18 - |expo|)
//	if |expo| > 18: price / 10^(|expo| - 18)
func pythPriceToContract(price *big.Int, expo int32) *big.Int {
	if price == nil || price.Sign() <= 0 {
		return nil
	}

	result := new(big.Int).Set(price)

	if expo >= 0 {
		// price * 10^18 * 10^expo = price * 10^(18+expo)
		exp := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(18+expo)), nil)
		result.Mul(result, exp)
	} else {
		absExpo := int32(-expo)
		if absExpo <= 18 {
			// price * 10^(18 - absExpo)
			exp := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(18-absExpo)), nil)
			result.Mul(result, exp)
		} else {
			// price / 10^(absExpo - 18)
			exp := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(absExpo-18)), nil)
			result.Div(result, exp)
		}
	}

	return result
}
