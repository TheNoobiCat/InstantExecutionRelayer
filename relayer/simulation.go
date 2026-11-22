package relayer

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"
)

var instantExecuteABI = `[{
	"inputs": [
		{"type": "address", "name": "target"},
		{"type": "uint256", "name": "value"},
		{"type": "bytes", "name": "data"},
		{"type": "uint256", "name": "nonce"},
		{"type": "bytes", "name": "signature"}
	],
	"name": "instantExecuteWithSignature",
	"outputs": [{"type": "bytes", "name": ""}],
	"stateMutability": "nonpayable",
	"type": "function"
}]`

var batchInstantExecuteABI = `[{
	"inputs": [
		{"type": "tuple[]", "name": "items", "components": [
			{"type": "address", "name": "target"},
			{"type": "uint256", "name": "value"},
			{"type": "bytes", "name": "data"}
		]},
		{"type": "uint256", "name": "nonce"},
		{"type": "bytes", "name": "signature"}
	],
	"name": "batchInstantExecuteWithSignature",
	"outputs": [{"type": "bytes[]", "name": ""}],
	"stateMutability": "nonpayable",
	"type": "function"
}]`

var channelABI = `[{
	"inputs": [],
	"name": "sessionKey",
	"outputs": [{"type": "address", "name": ""}],
	"stateMutability": "view",
	"type": "function"
}]`

func (r *Relayer) getSessionKey(ctx context.Context, channelAddr common.Address) (common.Address, error) {
	if r.ethClient == nil {
		return common.Address{}, fmt.Errorf("ethereum client not connected")
	}

	contractABI, err := abi.JSON(strings.NewReader(channelABI))
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to parse channel ABI: %v", err)
	}

	data, err := contractABI.Pack("sessionKey")
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to pack sessionKey call: %v", err)
	}

	callMsg := ethereum.CallMsg{
		To:   &channelAddr,
		Data: data,
	}

	ret, err := r.ethClient.CallContract(ctx, callMsg, nil)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to call sessionKey: %v", err)
	}

	results, err := contractABI.Unpack("sessionKey", ret)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to unpack sessionKey result: %v", err)
	}

	if len(results) == 0 {
		return common.Address{}, fmt.Errorf("no results from sessionKey call")
	}

	sessionKey, ok := results[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("invalid sessionKey result type")
	}

	return sessionKey, nil
}

func (r *Relayer) simulateContractCall(ctx context.Context, msg *SignedInstantExecute) error {
	if r.ethClient == nil {
		return fmt.Errorf("ethereum client not connected")
	}

	channel := common.HexToAddress(msg.Channel)
	target := common.HexToAddress(msg.Target)

	value, _ := r.parseWeiAmount(msg.Value)

	// Get the session key to use as From address
	sessionKey, err := r.getSessionKey(ctx, channel)
	if err != nil {
		return fmt.Errorf("failed to get session key: %v", err)
	}

	contractABI, err := abi.JSON(strings.NewReader(instantExecuteABI))
	if err != nil {
		return fmt.Errorf("failed to parse ABI: %v", err)
	}

	data, err := contractABI.Pack("instantExecuteWithSignature",
		target,
		value,
		msg.Data,
		big.NewInt(int64(msg.Nonce)),
		msg.Signature,
	)
	if err != nil {
		return fmt.Errorf("failed to pack function data: %v", err)
	}

	callMsg := ethereum.CallMsg{
		From: sessionKey,
		To:   &channel,
		Data: data,
		Gas:  500000,
	}

	ret, err := r.ethClient.CallContract(ctx, callMsg, nil)
	if err != nil {
		// Log full diagnostic details to help local debugging
		logrus.Infof("simulateContractCall failed: channel=%s target=%s err=%v", channel.Hex(), target.Hex(), err)
		logrus.Infof("calldata: %s", hex.EncodeToString(data))
		if len(ret) > 0 {
			logrus.Infof("revert returned bytes: %x", ret)
			if reason := decodeRevertFromBytes(ret); reason != "" {
				logrus.Infof("decoded revert reason: %s", reason)
				return fmt.Errorf("contract reverted: %s", reason)
			}
			// fallback: return hex-encoded revert
			return fmt.Errorf("contract reverted: revert data 0x%x", ret)
		}

		// Fallback to parsing err string
		revertReason := extractRevertReason(err)
		logrus.Infof("call error string: %s", err.Error())
		return fmt.Errorf("contract reverted: %s", revertReason)
	}

	return nil
}

func extractRevertReason(err error) string {
	errStr := err.Error()

	if strings.Contains(errStr, "execution reverted") {
		if strings.Contains(errStr, "0x") {
			hexPart := errStr[strings.Index(errStr, "0x"):]
			// Try to decode revert reason from ABI-encoded Error(string)
			// The payload typically starts with selector 0x08c379a0
			cleaned := strings.TrimPrefix(hexPart, "0x")
			b, decodeErr := hex.DecodeString(cleaned)
			if decodeErr == nil && len(b) >= 4 {
				// check for Error(string) selector
				if len(b) >= 4 && b[0] == 0x08 && b[1] == 0xc3 && b[2] == 0x79 && b[3] == 0xa0 {
					// ABI encoding: 4 selector + 32 offset + 32 length + data
					if len(b) >= 4+32+32 {
						// length is at bytes 4+32..4+32+32 (big endian)
						// parse as uint
						strLenBytes := b[4+32 : 4+32+32]
						// interpret last 8 bytes to get length (safe for reasonable lengths)
						var strLen int
						for i := 0; i < len(strLenBytes); i++ {
							strLen = strLen*256 + int(strLenBytes[i])
						}
						start := 4 + 32 + 32
						end := start + strLen
						if end <= len(b) {
							reason := string(b[start:end])
							return fmt.Sprintf("Revert reason: %s", reason)
						}
					}
				}
			}
			if len(hexPart) >= 10 {
				return fmt.Sprintf("Revert data: %s", hexPart[:min(66, len(hexPart))])
			}
		}
		return "Execution reverted"
	}

	return errStr
}

// decodeRevertFromBytes attempts to decode ABI-encoded Error(string) revert payload
// Returns empty string if it cannot decode a human-readable reason.
func decodeRevertFromBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	// The standard revert reason uses selector 0x08c379a0
	if len(b) >= 4 && b[0] == 0x08 && b[1] == 0xc3 && b[2] == 0x79 && b[3] == 0xa0 {
		// Skip selector
		payload := b[4:]
		// payload should be abi-encoded: offset(32) | length(32) | data
		if len(payload) < 64 {
			return ""
		}
		// read length from bytes 32..64
		lengthBytes := payload[32:64]
		strLen := 0
		for i := 0; i < len(lengthBytes); i++ {
			strLen = strLen*256 + int(lengthBytes[i])
		}
		start := 64
		end := start + strLen
		if end > len(payload) {
			// not enough data
			return ""
		}
		return string(payload[start:end])
	}
	// For custom errors or other reverts, return the selector
	if len(b) >= 4 {
		return fmt.Sprintf("custom error selector: %x", b[:4])
	}
	return ""
}

func (r *Relayer) simulateBatchContractCall(ctx context.Context, msg *SignedBatchInstantExecute) error {
	if r.ethClient == nil {
		return fmt.Errorf("ethereum client not connected")
	}

	channel := common.HexToAddress(msg.Channel)

	// Get the session key to use as From address
	sessionKey, err := r.getSessionKey(ctx, channel)
	if err != nil {
		return fmt.Errorf("failed to get session key: %v", err)
	}

	// Convert items to the format expected by the contract
	items := make([]struct {
		Target common.Address
		Value  *big.Int
		Data   []byte
	}, len(msg.Items))

	for i, item := range msg.Items {
		target := common.HexToAddress(item.Target)
		value, _ := r.parseWeiAmount(item.Value)
		items[i] = struct {
			Target common.Address
			Value  *big.Int
			Data   []byte
		}{
			Target: target,
			Value:  value,
			Data:   item.Data,
		}
	}

	contractABI, err := abi.JSON(strings.NewReader(batchInstantExecuteABI))
	if err != nil {
		return fmt.Errorf("failed to parse batch ABI: %v", err)
	}

	data, err := contractABI.Pack("batchInstantExecuteWithSignature",
		items,
		big.NewInt(int64(msg.Nonce)),
		msg.Signature,
	)
	if err != nil {
		return fmt.Errorf("failed to pack batch function data: %v", err)
	}

	callMsg := ethereum.CallMsg{
		From: sessionKey,
		To:   &channel,
		Data: data,
		Gas:  10000000, // Higher gas limit for batch operations
	}

	ret, err := r.ethClient.CallContract(ctx, callMsg, nil)
	if err != nil {
		logrus.Infof("simulateBatchContractCall failed: channel=%s err=%v", channel.Hex(), err)
		logrus.Infof("batch calldata: %s", hex.EncodeToString(data))
		if len(ret) > 0 {
			logrus.Infof("revert returned bytes: %x", ret)
			if reason := decodeRevertFromBytes(ret); reason != "" {
				logrus.Infof("decoded revert reason: %s", reason)
				return fmt.Errorf("contract reverted: %s", reason)
			}
			return fmt.Errorf("contract reverted: revert data 0x%x", ret)
		}

		revertReason := extractRevertReason(err)
		logrus.Infof("batch call error string: %s", err.Error())
		return fmt.Errorf("contract reverted: %s", revertReason)
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
