package executor

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/rs/zerolog"

	"github.com/thenoobicat/instantexecutionrelayer/keeper/config"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/metrics"
	"github.com/thenoobicat/instantexecutionrelayer/keeper/trigger"
)

// Executor manages transaction submission with proper nonce management,
// EIP-1559 gas pricing, and receipt tracking.
type Executor struct {
	ethClient  *ethclient.Client
	privateKey *ecdsa.PrivateKey
	fromAddr   common.Address
	chainID    *big.Int
	cfg        *config.Config
	logger     zerolog.Logger
	met        *metrics.Metrics

	// Nonce management: local nonce tracker to avoid races
	nonceMu   sync.Mutex
	nextNonce uint64
	nonceInit bool

	// Pending tx tracking
	pendingMu sync.Mutex
	pending   map[common.Hash]*PendingTx

	// Semaphore to limit concurrent pending txs
	sem chan struct{}
}

// PendingTx tracks a submitted transaction.
type PendingTx struct {
	Hash        common.Hash
	Order       *trigger.TriggerResult
	SubmittedAt time.Time
	Nonce       uint64
	GasPrice    *big.Int
}

// NewExecutor creates a new transaction executor.
func NewExecutor(
	ethClient *ethclient.Client,
	privateKey *ecdsa.PrivateKey,
	chainID *big.Int,
	cfg *config.Config,
	logger zerolog.Logger,
	met *metrics.Metrics,
) *Executor {
	fromAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	maxPending := cfg.Execution.MaxPendingTxs
	if maxPending <= 0 {
		maxPending = 4
	}

	return &Executor{
		ethClient:  ethClient,
		privateKey: privateKey,
		fromAddr:   fromAddr,
		chainID:    chainID,
		cfg:        cfg,
		logger:     logger.With().Str("component", "executor").Str("from", fromAddr.Hex()).Logger(),
		met:        met,
		pending:    make(map[common.Hash]*PendingTx),
		sem:        make(chan struct{}, maxPending),
	}
}

// FromAddress returns the keeper's address.
func (e *Executor) FromAddress() common.Address {
	return e.fromAddr
}

// Execute submits a simulated trigger result as an on-chain transaction.
// This is called by the trigger engine when simulation succeeds.
func (e *Executor) Execute(result *trigger.TriggerResult) {
	// Acquire semaphore slot (limits concurrent pending txs)
	select {
	case e.sem <- struct{}{}:
	default:
		e.logger.Warn().Str("order", result.Order.ID).Msg("max pending txs reached, dropping")
		e.met.OrdersFailed.WithLabelValues(result.Order.MarketSymbol, "max_pending").Inc()
		return
	}

	go func() {
		defer func() { <-e.sem }()
		e.executeWithRetry(result)
	}()
}

func (e *Executor) executeWithRetry(result *trigger.TriggerResult) {
	startTime := time.Now()
	order := result.Order

	// Get EIP-1559 gas parameters
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gasTipCap, gasFeeCap, err := e.getGasParams(ctx)
	if err != nil {
		e.logger.Error().Err(err).Str("order", order.ID).Msg("failed to get gas params")
		e.met.OrdersFailed.WithLabelValues(order.MarketSymbol, "gas_params").Inc()
		return
	}

	// Check gas cost against max
	mCfg := e.getMarketConfig(order.MarketSymbol)
	if mCfg != nil {
		if maxGas := mCfg.GetMaxGasWei(); maxGas != nil {
			estimatedCost := new(big.Int).Mul(gasFeeCap, big.NewInt(int64(result.GasEstimate)))
			if estimatedCost.Cmp(maxGas) > 0 {
				e.logger.Warn().
					Str("order", order.ID).
					Str("estimated_cost", estimatedCost.String()).
					Str("max_gas", maxGas.String()).
					Msg("gas cost exceeds maximum, skipping")
				e.met.OrdersFailed.WithLabelValues(order.MarketSymbol, "gas_too_high").Inc()
				return
			}
		}
	}

	// Get nonce
	nonce, err := e.getNextNonce(ctx)
	if err != nil {
		e.logger.Error().Err(err).Str("order", order.ID).Msg("failed to get nonce")
		e.met.OrdersFailed.WithLabelValues(order.MarketSymbol, "nonce_error").Inc()
		return
	}

	// Build EIP-1559 transaction
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   e.chainID,
		Nonce:     nonce,
		GasTipCap: gasTipCap,
		GasFeeCap: gasFeeCap,
		Gas:       result.GasEstimate,
		To:        &result.To,
		Value:     result.Value,
		Data:      result.Calldata,
	})

	// Sign
	signer := types.NewLondonSigner(e.chainID)
	signedTx, err := types.SignTx(tx, signer, e.privateKey)
	if err != nil {
		e.logger.Error().Err(err).Str("order", order.ID).Msg("failed to sign tx")
		e.met.OrdersFailed.WithLabelValues(order.MarketSymbol, "sign_error").Inc()
		e.releaseNonce(nonce)
		return
	}

	// Send
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer sendCancel()

	err = e.ethClient.SendTransaction(sendCtx, signedTx)
	if err != nil {
		errStr := err.Error()
		if isNonceError(errStr) {
			e.logger.Warn().Err(err).Str("order", order.ID).Uint64("tried_nonce", nonce).Msg("nonce error detected, resetting and will retry on next trigger")
			// Reset nonce tracker to fetch fresh from chain next time
			e.resetNonce()
			// Note: We don't retry immediately because:
			// 1. The order will be re-triggered on the next price update (it stays in the heap)
			// 2. Immediate retry could spam if there's a persistent issue
			// 3. The simulation will re-validate the order is still executable
		}
		e.logger.Error().Err(err).Str("order", order.ID).Str("tx", signedTx.Hash().Hex()).Msg("failed to send tx")
		e.met.OrdersFailed.WithLabelValues(order.MarketSymbol, "send_error").Inc()
		return
	}

	txHash := signedTx.Hash()
	e2eLatency := time.Since(result.Order.LastAttempt)
	e.met.E2ELatency.Observe(e2eLatency.Seconds())
	e.met.TxSubmitted.WithLabelValues(order.MarketSymbol).Inc()

	e.logger.Info().
		Str("order", order.ID).
		Str("type", order.Type.String()).
		Str("tx", txHash.Hex()).
		Uint64("nonce", nonce).
		Uint64("gas", result.GasEstimate).
		Str("gasFeeCap", gasFeeCap.String()).
		Dur("e2e_latency", e2eLatency).
		Msg("transaction submitted")

	// Track pending tx
	e.pendingMu.Lock()
	e.pending[txHash] = &PendingTx{
		Hash:        txHash,
		Order:       result,
		SubmittedAt: startTime,
		Nonce:       nonce,
		GasPrice:    gasFeeCap,
	}
	e.pendingMu.Unlock()

	// Wait for confirmation in background
	go e.waitForConfirmation(txHash, result)
}

// waitForConfirmation polls for tx receipt.
func (e *Executor) waitForConfirmation(txHash common.Hash, result *trigger.TriggerResult) {
	order := result.Order
	timeout := 60 * time.Second
	pollInterval := 500 * time.Millisecond
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		receipt, err := e.ethClient.TransactionReceipt(ctx, txHash)
		cancel()

		if err != nil {
			// Not mined yet, keep polling
			time.Sleep(pollInterval)
			continue
		}

		// Remove from pending
		e.pendingMu.Lock()
		delete(e.pending, txHash)
		e.pendingMu.Unlock()

		if receipt.Status == types.ReceiptStatusSuccessful {
			e.logger.Info().
				Str("order", order.ID).
				Str("type", order.Type.String()).
				Str("tx", txHash.Hex()).
				Uint64("gas_used", receipt.GasUsed).
				Uint64("block", receipt.BlockNumber.Uint64()).
				Msg("transaction confirmed successfully")

			e.met.TxConfirmed.WithLabelValues(order.MarketSymbol).Inc()
			e.met.OrdersExecuted.WithLabelValues(order.MarketSymbol, order.Type.String()).Inc()
			e.met.TxGasUsed.WithLabelValues(order.MarketSymbol, order.Type.String()).Observe(float64(receipt.GasUsed))
		} else {
			e.logger.Warn().
				Str("order", order.ID).
				Str("tx", txHash.Hex()).
				Uint64("gas_used", receipt.GasUsed).
				Msg("transaction reverted on-chain")

			e.met.TxFailed.WithLabelValues(order.MarketSymbol).Inc()
			e.met.OrdersFailed.WithLabelValues(order.MarketSymbol, "reverted").Inc()
		}
		return
	}

	e.logger.Warn().
		Str("order", order.ID).
		Str("tx", txHash.Hex()).
		Msg("transaction confirmation timed out")

	e.pendingMu.Lock()
	delete(e.pending, txHash)
	e.pendingMu.Unlock()

	e.met.OrdersFailed.WithLabelValues(order.MarketSymbol, "timeout").Inc()
}

// getGasParams returns EIP-1559 gas parameters with configured multiplier and buffer.
func (e *Executor) getGasParams(ctx context.Context) (gasTipCap, gasFeeCap *big.Int, err error) {
	// Get suggested priority fee
	gasTipCap, err = e.ethClient.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("suggest gas tip cap: %w", err)
	}

	// Cap priority fee
	maxTipWei := gweiToWei(e.cfg.Gas.MaxPriorityFeeGwei)
	if gasTipCap.Cmp(maxTipWei) > 0 {
		gasTipCap = maxTipWei
	}

	// Get base fee from latest header
	header, err := e.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("get latest header: %w", err)
	}

	baseFee := header.BaseFee
	if baseFee == nil {
		baseFee = big.NewInt(0)
	}

	// gasFeeCap = (baseFee + buffer) * multiplier + gasTipCap
	bufferWei := gweiToWei(e.cfg.Gas.BaseFeeBufferGwei)
	gasFeeCap = new(big.Int).Add(baseFee, bufferWei)

	// Apply multiplier
	if e.cfg.Gas.Multiplier > 1.0 {
		multiplied := new(big.Float).SetInt(gasFeeCap)
		multiplied.Mul(multiplied, big.NewFloat(e.cfg.Gas.Multiplier))
		gasFeeCap, _ = multiplied.Int(nil)
	}

	gasFeeCap.Add(gasFeeCap, gasTipCap)

	return gasTipCap, gasFeeCap, nil
}

// getNextNonce returns the next nonce to use, using local tracking to avoid races.
// If the local nonce drifts from chain state, it re-syncs automatically.
func (e *Executor) getNextNonce(ctx context.Context) (uint64, error) {
	e.nonceMu.Lock()
	defer e.nonceMu.Unlock()

	// Always fetch from chain if not initialized OR periodically to catch drift
	shouldRefresh := !e.nonceInit

	// Periodically verify nonce every ~20 transactions to catch drift from restarts/multiple instances
	if e.nonceInit && e.nextNonce%20 == 0 {
		shouldRefresh = true
	}

	if shouldRefresh {
		// Fetch fresh from chain
		nonce, err := e.ethClient.PendingNonceAt(ctx, e.fromAddr)
		if err != nil {
			return 0, fmt.Errorf("get pending nonce: %w", err)
		}

		if e.nonceInit && nonce != e.nextNonce {
			e.logger.Warn().
				Uint64("local_nonce", e.nextNonce).
				Uint64("chain_nonce", nonce).
				Msg("nonce drift detected, re-syncing from chain")
		}

		e.nextNonce = nonce
		e.nonceInit = true
	}

	nonce := e.nextNonce
	e.nextNonce++
	return nonce, nil
}

// releaseNonce decrements the nonce counter when a tx fails before sending.
func (e *Executor) releaseNonce(nonce uint64) {
	e.nonceMu.Lock()
	defer e.nonceMu.Unlock()
	// Only release if it's the most recent nonce
	if e.nextNonce == nonce+1 {
		e.nextNonce = nonce
	}
}

// resetNonce forces re-fetch of nonce from chain on next use.
func (e *Executor) resetNonce() {
	e.nonceMu.Lock()
	defer e.nonceMu.Unlock()
	e.nonceInit = false
}

func (e *Executor) getMarketConfig(symbol string) *config.MarketConfig {
	for i := range e.cfg.MonitoredMarkets {
		if e.cfg.MonitoredMarkets[i].MarketSymbol == symbol {
			return &e.cfg.MonitoredMarkets[i]
		}
	}
	return nil
}

// PendingCount returns the number of pending transactions.
func (e *Executor) PendingCount() int {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	return len(e.pending)
}

func gweiToWei(gwei float64) *big.Int {
	// Convert gwei to wei: gwei * 1e9
	gweiInt := int64(gwei * 1e9)
	return big.NewInt(gweiInt)
}

func isNonceError(errStr string) bool {
	noncePhrases := []string{
		"nonce too low",
		"nonce too high",
		"replacement transaction underpriced",
		"already known",
	}
	for _, phrase := range noncePhrases {
		if containsInsensitive(errStr, phrase) {
			return true
		}
	}
	return false
}

func containsInsensitive(s, substr string) bool {
	return len(s) >= len(substr) && containsLower(lower(s), lower(substr))
}

func lower(s string) string {
	b := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

func containsLower(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
