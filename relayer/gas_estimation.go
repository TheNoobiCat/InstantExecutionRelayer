package relayer

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// GasEstimationResult contains gas estimation details and calculated relayer fee
type GasEstimationResult struct {
	EstimatedGas  uint64   `json:"estimatedGas"`
	GasPrice      *big.Int `json:"gasPrice"`
	GasTipCap     *big.Int `json:"gasTipCap"`
	GasFeeCap     *big.Int `json:"gasFeeCap"`
	EstimatedCost *big.Int `json:"estimatedCost"` // (baseFee + gasTipCap) * gasLimit
}

// EstimateSessionCreationGas estimates the gas cost for a session creation transaction
func (r *Relayer) EstimateSessionCreationGas(ctx context.Context, msg *SignedSessionCreation) (*GasEstimationResult, error) {
	if r.ethClient == nil {
		return nil, fmt.Errorf("ethereum client not connected")
	}

	channel := common.HexToAddress(msg.Channel)
	sessionKey := common.HexToAddress(msg.SessionKey)

	contractABI, err := abi.JSON(strings.NewReader(reconnectWithSignatureABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ABI: %v", err)
	}

	// Validate expiry timestamp
	now := time.Now().Unix()
	expiry := now + msg.Duration
	if expiry <= now+60 { // Minimum 1 minute from now
		return nil, fmt.Errorf("session expiry too soon")
	}
	if expiry > now+30*24*3600 { // Maximum 30 days
		return nil, fmt.Errorf("session expiry too far in future")
	}

	// Use duration from message
	duration := msg.Duration
	if duration <= 60 { // Minimum 1 minute duration
		return nil, fmt.Errorf("session duration too short")
	}

	// Use the calculated duration
	durationBig := big.NewInt(duration)

	// Pack the transaction data
	data, err := contractABI.Pack("reconnectWithSignature",
		sessionKey,
		durationBig,
		big.NewInt(int64(msg.Nonce)),
		msg.Signature,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to pack function data: %v", err)
	}

	// Get current gas pricing from network (EIP-1559)
	gasTipCap, err := r.ethClient.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get gas tip cap: %v", err)
	}

	header, err := r.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get block header: %v", err)
	}

	gasFeeCap := new(big.Int).Add(header.BaseFee, gasTipCap)
	// Add a buffer to account for base fee increases (20% buffer)
	buffer := new(big.Int).Div(gasFeeCap, big.NewInt(5))
	gasFeeCap.Add(gasFeeCap, buffer)

	// Estimate gas limit for the transaction
	// Use a zero address as 'from' for estimation since we don't know the relayer address yet
	estimateMsg := ethereum.CallMsg{
		From: common.Address{}, // Zero address for estimation
		To:   &channel,
		Data: data,
	}

	gasLimit, err := r.ethClient.EstimateGas(ctx, estimateMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to estimate gas: %v", err)
	}

	// Add a small buffer to the gas estimate (10% safety margin)
	gasLimitWithBuffer := gasLimit + (gasLimit / 10)

	// Calculate total estimated cost using EIP-1559 pricing
	estimatedCost := new(big.Int).Mul(gasFeeCap, big.NewInt(int64(gasLimitWithBuffer)))

	return &GasEstimationResult{
		EstimatedGas:  gasLimitWithBuffer,
		GasPrice:      gasFeeCap, // For backward compatibility, use fee cap as gas price
		GasTipCap:     gasTipCap,
		GasFeeCap:     gasFeeCap,
		EstimatedCost: estimatedCost,
	}, nil
}
