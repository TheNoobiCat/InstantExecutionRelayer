package relayer

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sirupsen/logrus"
)

func (r *Relayer) SubmitSessionCreation(
	ctx context.Context,
	msg *SignedSessionCreation,
	privateKey *ecdsa.PrivateKey,
) (*SessionCreationResult, error) {
	logrus.Printf("SubmitSessionCreation: Starting submission process...")

	if r.ethClient == nil {
		return &SessionCreationResult{Success: false, Error: "ethereum client not connected"}, fmt.Errorf("ethereum client not connected")
	}

	if err := r.ValidateSessionCreation(ctx, msg); err != nil {
		logrus.Printf("SubmitSessionCreation: Validation failed: %v", err)
		return &SessionCreationResult{Success: false, Error: err.Error()}, err
	}

	logrus.Printf("SubmitSessionCreation: Validation passed, preparing transaction...")

	channel := common.HexToAddress(msg.Channel)
	sessionKey := common.HexToAddress(msg.SessionKey)

	contractABI, err := abi.JSON(strings.NewReader(reconnectWithSignatureABI))
	if err != nil {
		return &SessionCreationResult{Success: false, Error: err.Error()}, err
	}

	// Validate expiry timestamp
	now := time.Now().Unix()
	expiry := now + msg.Duration
	if expiry <= now+60 { // Minimum 1 minute from now
		return &SessionCreationResult{Success: false, Error: "session expiry too soon"}, nil
	}
	if expiry > now+30*24*3600 { // Maximum 30 days
		return &SessionCreationResult{Success: false, Error: "session expiry too far in future"}, nil
	}

	// Use duration from message
	duration := msg.Duration
	if duration <= 60 { // Minimum 1 minute duration
		return &SessionCreationResult{Success: false, Error: "session duration too short"}, nil
	}

	// Debug logging before packing
	logrus.Printf("About to pack ABI with values: sessionKey=%s, duration=%d, nonce=%d",
		sessionKey.Hex(), duration, msg.Nonce)

	data, err := contractABI.Pack("reconnectWithSignature",
		sessionKey,
		big.NewInt(duration),
		big.NewInt(int64(msg.Nonce)),
		msg.Signature,
	)
	if err != nil {
		return &SessionCreationResult{Success: false, Error: err.Error()}, err
	}

	fromAddr := crypto.PubkeyToAddress(privateKey.PublicKey)
	logrus.Printf("SubmitSessionCreation: From address: %s", fromAddr.Hex())

	gasLimit, err := r.ethClient.EstimateGas(ctx, ethereum.CallMsg{
		From: fromAddr,
		To:   &channel,
		Data: data,
	})
	if err != nil {
		logrus.Printf("SubmitSessionCreation: Gas estimation failed: %v", err)
		return &SessionCreationResult{Success: false, Error: err.Error()}, err
	}
	logrus.Printf("SubmitSessionCreation: Estimated gas: %d", gasLimit)

	nonce, err := r.ethClient.PendingNonceAt(ctx, fromAddr)
	if err != nil {
		logrus.Printf("SubmitSessionCreation: Nonce retrieval failed: %v", err)
		return &SessionCreationResult{Success: false, Error: err.Error()}, err
	}
	logrus.Printf("SubmitSessionCreation: Account nonce: %d", nonce)

	gasTipCap, err := r.ethClient.SuggestGasTipCap(ctx)
	if err != nil {
		return &SessionCreationResult{Success: false, Error: err.Error()}, err
	}

	header, err := r.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return &SessionCreationResult{Success: false, Error: err.Error()}, err
	}

	gasFeeCap := new(big.Int).Add(header.BaseFee, gasTipCap)
	// Add a buffer to account for base fee increases (20% buffer)
	buffer := new(big.Int).Div(gasFeeCap, big.NewInt(5))
	gasFeeCap.Add(gasFeeCap, buffer)
	logrus.Printf("SubmitSessionCreation: Gas tip cap: %s, Base fee: %s, Fee cap with buffer: %s", gasTipCap.String(), header.BaseFee.String(), gasFeeCap.String())

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   r.chainID,
		Nonce:     nonce,
		To:        &channel,
		Value:     big.NewInt(0),
		Gas:       gasLimit,
		GasFeeCap: gasFeeCap,
		GasTipCap: gasTipCap,
		Data:      data,
	})
	logrus.Printf("SubmitSessionCreation: Created EIP-1559 transaction to %s", channel.Hex())

	signedTx, err := types.SignTx(tx, types.NewLondonSigner(r.chainID), privateKey)
	if err != nil {
		logrus.Printf("SubmitSessionCreation: Transaction signing failed: %v", err)
		return &SessionCreationResult{Success: false, Error: err.Error()}, err
	}
	logrus.Printf("SubmitSessionCreation: Transaction signed, hash: %s", signedTx.Hash().Hex())

	if err := r.ethClient.SendTransaction(ctx, signedTx); err != nil {
		logrus.Printf("SubmitSessionCreation: Transaction send failed: %v", err)
		return &SessionCreationResult{Success: false, Error: err.Error()}, err
	}

	logrus.Printf("SubmitSessionCreation: Transaction sent successfully: %s", signedTx.Hash().Hex())
	return &SessionCreationResult{Success: true, TxHash: signedTx.Hash().Hex()}, nil
}
