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
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var reconnectWithSignatureABI = `[{
    "inputs": [
        {"type": "address", "name": "sessionKey"},
        {"type": "uint256", "name": "duration"},
        {"type": "uint256", "name": "nonce"},
        {"type": "bytes", "name": "signature"}
    ],
    "name": "reconnectWithSignature",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
}]`

func (r *Relayer) simulateSessionCreation(ctx context.Context, msg *SignedSessionCreation) error {
	if r.ethClient == nil {
		return fmt.Errorf("ethereum client not connected")
	}

	channel := common.HexToAddress(msg.Channel)
	sessionKey := common.HexToAddress(msg.SessionKey)

	contractABI, err := abi.JSON(strings.NewReader(reconnectWithSignatureABI))
	if err != nil {
		return fmt.Errorf("failed to parse ABI: %v", err)
	}

	// Validate expiry timestamp
	now := time.Now().Unix()
	expiry := now + msg.Duration
	if expiry <= now+60 { // Minimum 1 minute from now
		return fmt.Errorf("session expiry too soon")
	}
	if expiry > now+30*24*3600 { // Maximum 30 days
		return fmt.Errorf("session expiry too far in future")
	}

	// Use duration from message
	duration := msg.Duration
	if duration <= 60 { // Minimum 1 minute duration
		return fmt.Errorf("session duration too short")
	}

	// Debug logging before packing
	fmt.Printf("About to pack ABI with values: sessionKey=%s, duration=%d, nonce=%d\n",
		sessionKey.Hex(), duration, msg.Nonce)

	data, err := contractABI.Pack("reconnectWithSignature",
		sessionKey,
		big.NewInt(duration),
		big.NewInt(int64(msg.Nonce)),
		msg.Signature,
	)
	if err != nil {
		return fmt.Errorf("failed to pack function data: %v", err)
	}

	// Try to decode the packed args for better diagnostics
	if method, ok := contractABI.Methods["reconnectWithSignature"]; ok {
		if len(data) > 4 {
			// unpack expects the calldata without the 4-byte selector
			args, err := method.Inputs.Unpack(data[4:])
			if err == nil {
				// args: sessionKey, duration, nonce, signature
				sigLen := 0
				if b, ok := args[3].([]byte); ok {
					sigLen = len(b)
				}
				fmt.Printf("Decoded calldata args: sessionKey=%v duration=%v nonce=%v signature_len=%d\n", args[0], args[1], args[2], sigLen)
			} else {
				fmt.Printf("Failed to decode calldata args: %v\n", err)
			}
		}
	}

	callMsg := ethereum.CallMsg{
		From: common.HexToAddress("0x0000000000000000000000000000000000000000"),
		To:   &channel,
		Data: data,
		Gas:  300000,
	}

	result, err := r.ethClient.CallContract(ctx, callMsg, nil)
	if err != nil {
		// Log diagnostics to help debug why the simulation reverted
		fmt.Printf("simulateSessionCreation failed: channel=%s sessionKey=%s err=%v\n", channel.Hex(), sessionKey.Hex(), err)
		fmt.Printf("calldata: %s\n", hexutil.Encode(data))
		if len(result) > 0 {
			fmt.Printf("revert returned bytes: %x\n", result)
			if reason := decodeRevertFromBytes(result); reason != "" {
				fmt.Printf("decoded revert reason: %s\n", reason)
				return fmt.Errorf("contract reverted: %s", reason)
			}
			return fmt.Errorf("contract reverted: revert data 0x%x", result)
		}
		// fallback: parse the error string
		revertReason := extractRevertReason(err)
		fmt.Printf("call error string: %s\n", err.Error())
		return fmt.Errorf("contract reverted: %s", revertReason)
	}

	fmt.Printf("Session creation simulation successful! Return data: %s\n", hexutil.Encode(result))
	return nil
}
