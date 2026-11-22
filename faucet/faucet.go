// Package faucet provides a testnet USDC faucet endpoint protected by
// Cloudflare Turnstile captcha. Each wallet may claim once.
package faucet

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

var (
	// claimAmount is 1000 USDC (6 decimals).
	claimAmount = new(big.Int).Mul(big.NewInt(1000), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))

	// FaucetClaimsBucket stores claimed wallets (wallet address -> claimed flag)
	FaucetClaimsBucket = []byte("FaucetClaims")
)

// erc20TransferABI is the minimal ABI for ERC20 transfer(address,uint256).
const erc20TransferABI = `[{"inputs":[{"name":"to","type":"address"},{"name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"}]`

// Handler is the HTTP handler for the faucet endpoint.
type Handler struct {
	client          *ethclient.Client
	chainID         *big.Int
	privateKey      *ecdsa.PrivateKey
	usdcAddress     common.Address
	turnstileSecret string
	db              *bolt.DB

	mu sync.Mutex
}

// NewHandler creates a faucet handler. It reads from env:
//   - FAUCET_PRIVATE_KEY: hex private key of the funded wallet
//   - USDC_ADDRESS: ERC20 token address
//   - CAPTCHA_SECRET: Cloudflare Turnstile secret key
func NewHandler(client *ethclient.Client, chainID *big.Int) *Handler {
	// Load .env file to ensure environment variables are available
	_ = godotenv.Load()

	pkHex := os.Getenv("FAUCET_PRIVATE_KEY")
	if pkHex == "" {
		logrus.Warn("FAUCET_PRIVATE_KEY not set — faucet will reject all claims")
	}

	usdcAddr := os.Getenv("USDC_ADDRESS")
	if usdcAddr == "" {
		logrus.Warn("USDC_ADDRESS not set — faucet will reject all claims")
	}

	turnstileSecret := os.Getenv("CAPTCHA_SECRET")
	if turnstileSecret == "" {
		logrus.Warn("CAPTCHA_SECRET not set — faucet captcha verification will fail")
	}

	var pk *ecdsa.PrivateKey
	if pkHex != "" {
		pkHex = strings.TrimPrefix(pkHex, "0x")
		var err error
		pk, err = crypto.HexToECDSA(pkHex)
		if err != nil {
			logrus.WithError(err).Fatal("invalid FAUCET_PRIVATE_KEY")
		}
		addr := crypto.PubkeyToAddress(pk.PublicKey)
		logrus.WithField("faucet_wallet", addr.Hex()).Info("Faucet wallet loaded")
	}

	// Open bbolt database for faucet claims
	db, err := bolt.Open("faucet_claims.db", 0600, nil)
	if err != nil {
		logrus.WithError(err).Fatal("failed to open faucet claims database")
	}

	// Create bucket if it doesn't exist
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(FaucetClaimsBucket)
		return err
	})
	if err != nil {
		logrus.WithError(err).Fatal("failed to create faucet claims bucket")
	}

	h := &Handler{
		client:          client,
		chainID:         chainID,
		privateKey:      pk,
		usdcAddress:     common.HexToAddress(usdcAddr),
		turnstileSecret: turnstileSecret,
		db:              db,
	}

	// Log number of claims loaded from database
	var count int
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(FaucetClaimsBucket)
		count = b.Stats().KeyN
		return nil
	})
	logrus.WithField("count", count).Info("loaded persisted faucet claims from bbolt")

	return h
}

// Close closes the database connection
func (h *Handler) Close() error {
	if h.db != nil {
		return h.db.Close()
	}
	return nil
}

// hasClaimed checks if a wallet has already claimed
func (h *Handler) hasClaimed(wallet string) (bool, error) {
	var claimed bool
	err := h.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(FaucetClaimsBucket)
		val := b.Get([]byte(wallet))
		claimed = val != nil
		return nil
	})
	return claimed, err
}

// markClaimed marks a wallet as having claimed
func (h *Handler) markClaimed(wallet string) error {
	return h.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(FaucetClaimsBucket)
		return b.Put([]byte(wallet), []byte{1})
	})
}

// unmarkClaimed removes the claimed status for a wallet (used for rollback)
func (h *Handler) unmarkClaimed(wallet string) error {
	return h.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(FaucetClaimsBucket)
		return b.Delete([]byte(wallet))
	})
}

// request is the expected JSON body.
type request struct {
	Wallet         string `json:"wallet"`
	TurnstileToken string `json:"turnstileToken"`
}

// response is the JSON response.
type response struct {
	Success bool   `json:"success"`
	TxHash  string `json:"txHash,omitempty"`
	Error   string `json:"error,omitempty"`
	Amount  string `json:"amount,omitempty"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if !common.IsHexAddress(req.Wallet) {
		writeError(w, http.StatusBadRequest, "invalid wallet address")
		return
	}
	wallet := strings.ToLower(req.Wallet)

	// Verify Turnstile captcha
	if err := h.verifyTurnstile(req.TurnstileToken, r.RemoteAddr); err != nil {
		writeError(w, http.StatusForbidden, fmt.Sprintf("captcha verification failed: %v", err))
		return
	}

	// Check if already claimed
	h.mu.Lock()
	claimed, err := h.hasClaimed(wallet)
	if err != nil {
		h.mu.Unlock()
		logrus.WithError(err).Error("failed to check claim status")
		writeError(w, http.StatusInternalServerError, "failed to check claim status")
		return
	}
	if claimed {
		h.mu.Unlock()
		writeError(w, http.StatusConflict, "wallet has already claimed faucet tokens")
		return
	}
	// Mark as claimed immediately to prevent double-claims from concurrent requests
	if err := h.markClaimed(wallet); err != nil {
		h.mu.Unlock()
		logrus.WithError(err).Error("failed to mark wallet as claimed")
		writeError(w, http.StatusInternalServerError, "failed to mark wallet as claimed")
		return
	}
	h.mu.Unlock()

	// Send USDC
	txHash, err := h.sendUSDC(common.HexToAddress(req.Wallet))
	if err != nil {
		// Revert claim on failure
		h.mu.Lock()
		if err := h.unmarkClaimed(wallet); err != nil {
			logrus.WithError(err).Warn("failed to revert claim status after transfer failure")
		}
		h.mu.Unlock()

		logrus.WithError(err).WithField("wallet", wallet).Error("faucet transfer failed")
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("transfer failed: %v", err))
		return
	}

	logrus.WithFields(logrus.Fields{
		"wallet": wallet,
		"txHash": txHash,
		"amount": "1000 USDC",
	}).Info("faucet claim successful")

	json.NewEncoder(w).Encode(response{
		Success: true,
		TxHash:  txHash,
		Amount:  "1000",
	})
}

func (h *Handler) sendUSDC(to common.Address) (string, error) {
	if h.privateKey == nil {
		return "", fmt.Errorf("faucet wallet not configured")
	}

	parsed, err := abi.JSON(strings.NewReader(erc20TransferABI))
	if err != nil {
		return "", fmt.Errorf("parse ABI: %w", err)
	}

	calldata, err := parsed.Pack("transfer", to, claimAmount)
	if err != nil {
		return "", fmt.Errorf("pack calldata: %w", err)
	}

	ctx := context.Background()
	fromAddr := crypto.PubkeyToAddress(h.privateKey.PublicKey)

	nonce, err := h.client.PendingNonceAt(ctx, fromAddr)
	if err != nil {
		return "", fmt.Errorf("get nonce: %w", err)
	}

	head, err := h.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("get header: %w", err)
	}

	tipCap, err := h.client.SuggestGasTipCap(ctx)
	if err != nil {
		tipCap = big.NewInt(100_000_000) // 0.1 gwei fallback
	}

	baseFee := head.BaseFee
	if baseFee == nil {
		baseFee = big.NewInt(1_000_000_000)
	}
	// maxFee = 2 * baseFee + tipCap
	maxFee := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tipCap)

	gasLimit := uint64(80_000) // ERC20 transfer typically uses ~60k

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   h.chainID,
		Nonce:     nonce,
		GasTipCap: tipCap,
		GasFeeCap: maxFee,
		Gas:       gasLimit,
		To:        &h.usdcAddress,
		Value:     big.NewInt(0),
		Data:      calldata,
	})

	signer := types.LatestSignerForChainID(h.chainID)
	signedTx, err := types.SignTx(tx, signer, h.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign tx: %w", err)
	}

	if err := h.client.SendTransaction(ctx, signedTx); err != nil {
		return "", fmt.Errorf("send tx: %w", err)
	}

	// Wait for receipt
	receipt, err := bind.WaitMined(ctx, h.client, signedTx)
	if err != nil {
		return signedTx.Hash().Hex(), fmt.Errorf("wait mined: %w", err)
	}

	if receipt.Status != types.ReceiptStatusSuccessful {
		return signedTx.Hash().Hex(), fmt.Errorf("tx reverted")
	}

	return signedTx.Hash().Hex(), nil
}

// verifyTurnstile validates the Cloudflare Turnstile token.
func (h *Handler) verifyTurnstile(token, remoteIP string) error {
	if h.turnstileSecret == "" {
		return fmt.Errorf("turnstile secret not configured")
	}
	if token == "" {
		return fmt.Errorf("missing turnstile token")
	}

	body := fmt.Sprintf("secret=%s&response=%s&remoteip=%s",
		h.turnstileSecret, token, remoteIP)

	resp, err := http.Post(
		"https://challenges.cloudflare.com/turnstile/v0/siteverify",
		"application/x-www-form-urlencoded",
		strings.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("turnstile request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Success bool `json:"success"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("turnstile decode: %w", err)
	}
	if !result.Success {
		return fmt.Errorf("turnstile rejected token")
	}
	return nil
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(response{Success: false, Error: msg})
}
