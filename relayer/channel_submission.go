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

// deployChannelForABI is the ABI for the deployChannelFor function
const deployChannelForABI = `[
	{
		"inputs": [
			{"internalType": "address", "name": "user", "type": "address"},
			{"internalType": "uint256", "name": "nonce", "type": "uint256"},
			{"internalType": "bytes", "name": "signature", "type": "bytes"}
		],
		"outputs": [
			{"internalType": "address", "name": "channel", "type": "address"}
		],
		"name": "deployChannelFor",
		"stateMutability": "nonpayable",
		"type": "function"
	}
]`

// batchDeployForABI is the ABI for the batchDeployFor function
const batchDeployForABI = `[
	{
		"inputs": [
			{"internalType": "address[]", "name": "users", "type": "address[]"},
			{"internalType": "uint256[]", "name": "nonces", "type": "uint256[]"},
			{"internalType": "bytes[]", "name": "signatures", "type": "bytes[]"}
		],
		"outputs": [
			{"internalType": "address[]", "name": "channels", "type": "address[]"}
		],
		"name": "batchDeployFor",
		"stateMutability": "nonpayable",
		"type": "function"
	}
]`

func (r *Relayer) SubmitChannelCreation(
	ctx context.Context,
	msg *SignedChannelCreation,
	privateKey *ecdsa.PrivateKey,
) (*ChannelCreationResult, error) {
	logrus.Infof("SubmitChannelCreation: Starting channel creation process for user %s", msg.User)

	if r.ethClient == nil {
		return &ChannelCreationResult{Success: false, Error: "ethereum client not connected"}, fmt.Errorf("ethereum client not connected")
	}

	if err := r.ValidateChannelCreation(ctx, msg); err != nil {
		logrus.Infof("SubmitChannelCreation: Validation failed: %v", err)
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	logrus.Infof("SubmitChannelCreation: Validation passed, preparing transaction...")

	user := common.HexToAddress(msg.User)

	contractABI, err := abi.JSON(strings.NewReader(deployChannelForABI))
	if err != nil {
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	data, err := contractABI.Pack("deployChannelFor",
		user,
		big.NewInt(int64(msg.Nonce)),
		msg.Signature,
	)
	if err != nil {
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	fromAddr := crypto.PubkeyToAddress(privateKey.PublicKey)
	logrus.Infof("SubmitChannelCreation: From address: %s", fromAddr.Hex())

	gasLimit, err := r.ethClient.EstimateGas(ctx, ethereum.CallMsg{
		From: fromAddr,
		To:   &r.factoryAddress,
		Data: data,
	})
	if err != nil {
		logrus.Infof("SubmitChannelCreation: Gas estimation failed: %v", err)
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}
	logrus.Infof("SubmitChannelCreation: Estimated gas: %d", gasLimit)

	nonce, err := r.ethClient.PendingNonceAt(ctx, fromAddr)
	if err != nil {
		logrus.Infof("SubmitChannelCreation: Nonce retrieval failed: %v", err)
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}
	logrus.Infof("SubmitChannelCreation: Account nonce: %d", nonce)

	gasTipCap, err := r.ethClient.SuggestGasTipCap(ctx)
	if err != nil {
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	header, err := r.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	gasFeeCap := new(big.Int).Add(header.BaseFee, gasTipCap)
	// Add a buffer to account for base fee increases (20% buffer)
	buffer := new(big.Int).Div(gasFeeCap, big.NewInt(5))
	gasFeeCap.Add(gasFeeCap, buffer)
	logrus.Infof("SubmitChannelCreation: Gas tip cap: %s, Base fee: %s, Fee cap with buffer: %s", gasTipCap.String(), header.BaseFee.String(), gasFeeCap.String())

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   r.chainID,
		Nonce:     nonce,
		To:        &r.factoryAddress,
		Value:     big.NewInt(0),
		Gas:       gasLimit,
		GasFeeCap: gasFeeCap,
		GasTipCap: gasTipCap,
		Data:      data,
	})
	logrus.Infof("SubmitChannelCreation: Created EIP-1559 transaction to factory %s", r.factoryAddress.Hex())

	signedTx, err := types.SignTx(tx, types.NewLondonSigner(r.chainID), privateKey)
	if err != nil {
		logrus.Infof("SubmitChannelCreation: Transaction signing failed: %v", err)
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}
	logrus.Infof("SubmitChannelCreation: Transaction signed, hash: %s", signedTx.Hash().Hex())

	if err := r.ethClient.SendTransaction(ctx, signedTx); err != nil {
		logrus.Infof("SubmitChannelCreation: Transaction send failed: %v", err)
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	logrus.Infof("SubmitChannelCreation: Transaction sent successfully: %s", signedTx.Hash().Hex())

	// Wait for transaction confirmation
	receipt, err := r.waitForConfirmation(ctx, signedTx.Hash())
	if err != nil {
		logrus.Infof("SubmitChannelCreation: Confirmation failed: %v", err)
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	if receipt.Status != 1 {
		logrus.Infof("SubmitChannelCreation: Transaction failed with status %d", receipt.Status)
		return &ChannelCreationResult{Success: false, Error: "transaction reverted"}, nil
	}

	// Extract channel address from logs
	channelAddr, err := r.extractChannelAddressFromReceipt(receipt)
	if err != nil {
		logrus.Infof("SubmitChannelCreation: Failed to extract channel address: %v", err)
		return &ChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	logrus.Infof("SubmitChannelCreation: Channel created successfully: %s", channelAddr.Hex())

	// Notify the client that the channel was created
	if conn, ok := r.PendingConnections[msg.Nonce]; ok {
		_ = conn.SendJSON("channel_created", map[string]interface{}{
			"channelAddress": channelAddr.Hex(),
			"txHash":         signedTx.Hash().Hex(),
		})
		delete(r.PendingConnections, msg.Nonce)
	}

	return &ChannelCreationResult{
		Success: true,
		TxHash:  signedTx.Hash().Hex(),
		Channel: channelAddr.Hex(),
	}, nil
}

func (r *Relayer) SubmitBatchChannelCreation(
	ctx context.Context,
	msg *BatchChannelCreation,
	privateKey *ecdsa.PrivateKey,
) (*BatchChannelCreationResult, error) {
	logrus.Infof("SubmitBatchChannelCreation: Starting batch channel creation for %d users", len(msg.Users))

	if r.ethClient == nil {
		return &BatchChannelCreationResult{Success: false, Error: "ethereum client not connected"}, fmt.Errorf("ethereum client not connected")
	}

	if err := r.ValidateBatchChannelCreation(ctx, msg); err != nil {
		logrus.Infof("SubmitBatchChannelCreation: Validation failed: %v", err)
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	logrus.Infof("SubmitBatchChannelCreation: Validation passed, preparing transaction...")

	// Convert users to addresses
	users := make([]common.Address, len(msg.Users))
	for i, user := range msg.Users {
		users[i] = common.HexToAddress(user)
	}

	// Convert nonces to big ints
	nonces := make([]*big.Int, len(msg.Nonces))
	for i, nonce := range msg.Nonces {
		nonces[i] = big.NewInt(int64(nonce))
	}

	contractABI, err := abi.JSON(strings.NewReader(batchDeployForABI))
	if err != nil {
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	data, err := contractABI.Pack("batchDeployFor",
		users,
		nonces,
		msg.Signatures,
	)
	if err != nil {
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	fromAddr := crypto.PubkeyToAddress(privateKey.PublicKey)
	logrus.Infof("SubmitBatchChannelCreation: From address: %s", fromAddr.Hex())

	gasLimit, err := r.ethClient.EstimateGas(ctx, ethereum.CallMsg{
		From: fromAddr,
		To:   &r.factoryAddress,
		Data: data,
	})
	if err != nil {
		logrus.Infof("SubmitBatchChannelCreation: Gas estimation failed: %v", err)
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}
	logrus.Infof("SubmitBatchChannelCreation: Estimated gas: %d", gasLimit)

	nonce, err := r.ethClient.PendingNonceAt(ctx, fromAddr)
	if err != nil {
		logrus.Infof("SubmitBatchChannelCreation: Nonce retrieval failed: %v", err)
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}
	logrus.Infof("SubmitBatchChannelCreation: Account nonce: %d", nonce)

	gasTipCap, err := r.ethClient.SuggestGasTipCap(ctx)
	if err != nil {
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	header, err := r.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	gasFeeCap := new(big.Int).Add(header.BaseFee, gasTipCap)
	// Add a buffer to account for base fee increases (20% buffer)
	buffer := new(big.Int).Div(gasFeeCap, big.NewInt(5))
	gasFeeCap.Add(gasFeeCap, buffer)
	logrus.Infof("SubmitBatchChannelCreation: Gas tip cap: %s, Base fee: %s, Fee cap with buffer: %s", gasTipCap.String(), header.BaseFee.String(), gasFeeCap.String())

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   r.chainID,
		Nonce:     nonce,
		To:        &r.factoryAddress,
		Value:     big.NewInt(0),
		Gas:       gasLimit,
		GasFeeCap: gasFeeCap,
		GasTipCap: gasTipCap,
		Data:      data,
	})
	logrus.Infof("SubmitBatchChannelCreation: Created EIP-1559 transaction to factory %s", r.factoryAddress.Hex())

	signedTx, err := types.SignTx(tx, types.NewLondonSigner(r.chainID), privateKey)
	if err != nil {
		logrus.Infof("SubmitBatchChannelCreation: Transaction signing failed: %v", err)
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}
	logrus.Infof("SubmitBatchChannelCreation: Transaction signed, hash: %s", signedTx.Hash().Hex())

	if err := r.ethClient.SendTransaction(ctx, signedTx); err != nil {
		logrus.Infof("SubmitBatchChannelCreation: Transaction send failed: %v", err)
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	logrus.Infof("SubmitBatchChannelCreation: Transaction sent successfully: %s", signedTx.Hash().Hex())

	// Wait for transaction confirmation
	receipt, err := r.waitForConfirmation(ctx, signedTx.Hash())
	if err != nil {
		logrus.Infof("SubmitBatchChannelCreation: Confirmation failed: %v", err)
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	if receipt.Status != 1 {
		logrus.Infof("SubmitBatchChannelCreation: Transaction failed with status %d", receipt.Status)
		return &BatchChannelCreationResult{Success: false, Error: "transaction reverted"}, nil
	}

	// Extract channel addresses from logs
	channelAddrs, err := r.extractBatchChannelAddressesFromReceipt(receipt, len(msg.Users))
	if err != nil {
		logrus.Infof("SubmitBatchChannelCreation: Failed to extract channel addresses: %v", err)
		return &BatchChannelCreationResult{Success: false, Error: err.Error()}, err
	}

	channelHexes := make([]string, len(channelAddrs))
	for i, addr := range channelAddrs {
		channelHexes[i] = addr.Hex()
	}

	logrus.Infof("SubmitBatchChannelCreation: Batch channels created successfully: %v", channelHexes)
	return &BatchChannelCreationResult{
		Success:  true,
		TxHash:   signedTx.Hash().Hex(),
		Channels: channelHexes,
	}, nil
}

// waitForConfirmation waits for a transaction to be confirmed
func (r *Relayer) waitForConfirmation(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	if r.ethClient == nil {
		return nil, fmt.Errorf("ethereum client not connected")
	}

	for {
		receipt, err := r.ethClient.TransactionReceipt(ctx, txHash)
		if err == nil {
			return receipt, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
			// Continue polling
		}
	}
}

// extractChannelAddressFromReceipt extracts the channel address from a ChannelCreated event
func (r *Relayer) extractChannelAddressFromReceipt(receipt *types.Receipt) (common.Address, error) {
	// ChannelCreated event signature: ChannelCreated(address indexed user, address channel, address indexed deployer)
	eventSig := crypto.Keccak256Hash([]byte("ChannelCreated(address,address,address)"))

	for _, log := range receipt.Logs {
		if log.Topics[0] == eventSig {
			// The channel address is the second indexed parameter (topics[2])
			if len(log.Topics) >= 3 {
				return common.HexToAddress(log.Topics[2].Hex()), nil
			}
		}
	}

	return common.Address{}, fmt.Errorf("ChannelCreated event not found in receipt")
}

// extractBatchChannelAddressesFromReceipt extracts multiple channel addresses from batch ChannelCreated events
func (r *Relayer) extractBatchChannelAddressesFromReceipt(receipt *types.Receipt, expectedCount int) ([]common.Address, error) {
	eventSig := crypto.Keccak256Hash([]byte("ChannelCreated(address,address,address)"))

	var channels []common.Address
	for _, logEntry := range receipt.Logs {
		if logEntry.Topics[0] == eventSig {
			if len(logEntry.Topics) >= 3 {
				channels = append(channels, common.HexToAddress(logEntry.Topics[2].Hex()))
			}
		}
	}

	if len(channels) != expectedCount {
		return nil, fmt.Errorf("expected %d channels, found %d in logs", expectedCount, len(channels))
	}

	return channels, nil
}

// SubmitSignedInstantExecute submits a validated instant execute transaction to the blockchain
func (r *Relayer) SubmitSignedInstantExecute(
	ctx context.Context,
	msg *SignedInstantExecute,
) (string, error) {
	logrus.Infof("SubmitSignedInstantExecute: Starting instant execute submission for channel %s", msg.Channel)

	if r.ethClient == nil {
		return "", fmt.Errorf("ethereum client not connected")
	}

	if r.privateKey == nil {
		return "", fmt.Errorf("private key not configured")
	}

	channel := common.HexToAddress(msg.Channel)
	target := common.HexToAddress(msg.Target)
	value, _ := r.parseWeiAmount(msg.Value)

	contractABI, err := abi.JSON(strings.NewReader(instantExecuteABI))
	if err != nil {
		return "", fmt.Errorf("failed to parse ABI: %v", err)
	}

	data, err := contractABI.Pack("instantExecuteWithSignature",
		target,
		value,
		msg.Data,
		big.NewInt(int64(msg.Nonce)),
		msg.Signature,
	)
	if err != nil {
		return "", fmt.Errorf("failed to pack function data: %v", err)
	}

	fromAddr := crypto.PubkeyToAddress(r.privateKey.PublicKey)
	logrus.Infof("SubmitSignedInstantExecute: From address: %s", fromAddr.Hex())

	gasLimit, err := r.ethClient.EstimateGas(ctx, ethereum.CallMsg{
		From: fromAddr,
		To:   &channel,
		Data: data,
	})
	if err != nil {
		logrus.Infof("SubmitSignedInstantExecute: Gas estimation failed: %v", err)
		return "", err
	}
	logrus.Infof("SubmitSignedInstantExecute: Estimated gas: %d", gasLimit)

	nonce, err := r.ethClient.PendingNonceAt(ctx, fromAddr)
	if err != nil {
		logrus.Infof("SubmitSignedInstantExecute: Nonce retrieval failed: %v", err)
		return "", err
	}

	// Get latest block header for base fee
	header, err := r.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		logrus.Infof("SubmitSignedInstantExecute: Failed to get latest block header: %v", err)
		return "", err
	}

	baseFee := header.BaseFee
	gasTipCap, err := r.ethClient.SuggestGasTipCap(ctx)
	if err != nil {
		logrus.Infof("SubmitSignedInstantExecute: Gas tip cap suggestion failed: %v", err)
		return "", err
	}

	// Calculate maxFeePerGas as baseFee + tip + buffer
	maxFeePerGas := new(big.Int).Add(baseFee, gasTipCap)
	maxFeePerGas.Add(maxFeePerGas, big.NewInt(10000000000)) // Add 10 gwei buffer

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   r.chainID,
		Nonce:     nonce,
		GasTipCap: gasTipCap,
		GasFeeCap: maxFeePerGas,
		Gas:       gasLimit,
		To:        &channel,
		Value:     big.NewInt(0),
		Data:      data,
	})

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(r.chainID), r.privateKey)
	if err != nil {
		logrus.Infof("SubmitSignedInstantExecute: Transaction signing failed: %v", err)
		return "", err
	}

	logrus.Infof("SubmitSignedInstantExecute: Sending transaction %s", signedTx.Hash().Hex())
	err = r.ethClient.SendTransaction(ctx, signedTx)
	if err != nil {
		logrus.Infof("SubmitSignedInstantExecute: Transaction send failed: %v", err)
		return "", err
	}

	logrus.Infof("SubmitSignedInstantExecute: Transaction submitted: %s", signedTx.Hash().Hex())
	return signedTx.Hash().Hex(), nil
}

// SubmitBatchInstantExecute submits a validated batch instant execute transaction to the blockchain
func (r *Relayer) SubmitBatchInstantExecute(
	ctx context.Context,
	msg *SignedBatchInstantExecute,
) (string, error) {
	logrus.Infof("SubmitBatchInstantExecute: Starting batch instant execute submission for channel %s", msg.Channel)

	if r.ethClient == nil {
		return "", fmt.Errorf("ethereum client not connected")
	}

	if r.privateKey == nil {
		return "", fmt.Errorf("private key not configured")
	}

	channel := common.HexToAddress(msg.Channel)

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
		return "", fmt.Errorf("failed to parse batch ABI: %v", err)
	}

	data, err := contractABI.Pack("batchInstantExecuteWithSignature",
		items,
		big.NewInt(int64(msg.Nonce)),
		msg.Signature,
	)
	if err != nil {
		return "", fmt.Errorf("failed to pack batch function data: %v", err)
	}

	fromAddr := crypto.PubkeyToAddress(r.privateKey.PublicKey)
	logrus.Infof("SubmitBatchInstantExecute: From address: %s", fromAddr.Hex())

	gasLimit, err := r.ethClient.EstimateGas(ctx, ethereum.CallMsg{
		From: fromAddr,
		To:   &channel,
		Data: data,
	})
	if err != nil {
		logrus.Infof("SubmitBatchInstantExecute: Gas estimation failed: %v", err)
		return "", err
	}
	logrus.Infof("SubmitBatchInstantExecute: Estimated gas: %d", gasLimit)

	nonce, err := r.ethClient.PendingNonceAt(ctx, fromAddr)
	if err != nil {
		logrus.Infof("SubmitBatchInstantExecute: Nonce retrieval failed: %v", err)
		return "", err
	}

	// Get current block for base fee
	header, err := r.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		logrus.Infof("SubmitBatchInstantExecute: Header retrieval failed: %v", err)
		return "", err
	}
	baseFee := header.BaseFee

	// Suggest gas tip cap
	gasTipCap, err := r.ethClient.SuggestGasTipCap(ctx)
	if err != nil {
		logrus.Infof("SubmitBatchInstantExecute: Gas tip cap suggestion failed: %v", err)
		return "", err
	}

	// Calculate max fee per gas (base fee + tip + buffer)
	maxFeePerGas := new(big.Int).Add(baseFee, gasTipCap)
	maxFeePerGas.Add(maxFeePerGas, big.NewInt(1000000000)) // Add 1 gwei buffer

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   r.chainID,
		Nonce:     nonce,
		To:        &channel,
		Value:     big.NewInt(0),
		Gas:       gasLimit,
		GasFeeCap: maxFeePerGas,
		GasTipCap: gasTipCap,
		Data:      data,
	})

	signedTx, err := types.SignTx(tx, types.NewLondonSigner(r.chainID), r.privateKey)
	if err != nil {
		logrus.Infof("SubmitBatchInstantExecute: Transaction signing failed: %v", err)
		return "", err
	}

	logrus.Infof("SubmitBatchInstantExecute: Sending transaction %s", signedTx.Hash().Hex())
	err = r.ethClient.SendTransaction(ctx, signedTx)
	if err != nil {
		logrus.Infof("SubmitBatchInstantExecute: Transaction send failed: %v", err)
		return "", err
	}

	logrus.Infof("SubmitBatchInstantExecute: Transaction submitted: %s", signedTx.Hash().Hex())
	return signedTx.Hash().Hex(), nil
}

// WaitForReceipt waits for a transaction to be confirmed on the blockchain
func (r *Relayer) WaitForReceipt(ctx context.Context, txHash string) (*types.Receipt, error) {
	logrus.Infof("waitForReceipt: Waiting for confirmation of transaction %s", txHash)

	timeout := time.After(30 * time.Second)          // 30 second timeout
	ticker := time.NewTicker(500 * time.Millisecond) // Check every half second
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return nil, fmt.Errorf("timeout waiting for transaction confirmation")
		case <-ticker.C:
			receipt, err := r.ethClient.TransactionReceipt(ctx, common.HexToHash(txHash))
			if err == nil && receipt != nil {
				if receipt.Status == types.ReceiptStatusSuccessful {
					logrus.Infof("waitForReceipt: Transaction %s confirmed successfully", txHash)
					return receipt, nil
				} else {
					logrus.Infof("waitForReceipt: Transaction %s failed with status %d", txHash, receipt.Status)
					return nil, fmt.Errorf("transaction failed")
				}
			}
			// If err != nil or receipt == nil, continue polling
		}
	}
}
