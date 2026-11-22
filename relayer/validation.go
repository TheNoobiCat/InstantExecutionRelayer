package relayer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sirupsen/logrus"
)

var (
	ErrMessageExpired       = errors.New("message expired")
	ErrInvalidVersion       = errors.New("unsupported version")
	ErrInvalidType          = errors.New("invalid message type")
	ErrInvalidSignature     = errors.New("invalid signature")
	ErrInvalidAddress       = errors.New("invalid address")
	ErrInvalidAmount        = errors.New("invalid amount")
	ErrChainIDMismatch      = errors.New("chain ID mismatch")
	ErrSimulationFailed     = errors.New("contract simulation failed")
	ErrInvalidChannel       = errors.New("invalid channel address")
	ErrChannelCodeMismatch  = errors.New("channel code doesn't match expected implementation")
	ErrFactoryNotConfigured = errors.New("factory address not configured")
	ErrNotAContract         = errors.New("address is not a contract")
)

func (r *Relayer) ValidateSignedMessage(ctx context.Context, msg *SignedInstantExecute) error {
	if err := r.validateBasicFields(msg); err != nil {
		return err
	}

	// Validate channel address
	// if err := r.ValidateChannelAddress(ctx, msg.Channel); err != nil {
	// 	return fmt.Errorf("channel validation failed: %w", err)
	// }

	if time.Now().Unix() > msg.Expires {
		return ErrMessageExpired
	}

	if !common.IsHexAddress(msg.Channel) || !common.IsHexAddress(msg.Target) {
		return ErrInvalidAddress
	}

	if _, err := r.parseWeiAmount(msg.Value); err != nil {
		return fmt.Errorf("invalid value: %w", err)
	}

	if msg.ChainID != r.chainID.Uint64() {
		return ErrChainIDMismatch
	}

	if err := r.simulateContractCall(ctx, msg); err != nil {
		return fmt.Errorf("%w: %v", ErrSimulationFailed, err)
	}

	return nil
}

func (r *Relayer) ValidateSignedBatchMessage(ctx context.Context, msg *SignedBatchInstantExecute) error {
	if err := r.validateBasicBatchFields(msg); err != nil {
		return err
	}

	if time.Now().Unix() > msg.Expires {
		return ErrMessageExpired
	}

	if !common.IsHexAddress(msg.Channel) {
		return ErrInvalidAddress
	}

	for _, item := range msg.Items {
		if !common.IsHexAddress(item.Target) {
			return ErrInvalidAddress
		}
		if _, err := r.parseWeiAmount(item.Value); err != nil {
			return fmt.Errorf("invalid value in batch item: %w", err)
		}
	}

	if msg.ChainID != r.chainID.Uint64() {
		return ErrChainIDMismatch
	}

	if err := r.simulateBatchContractCall(ctx, msg); err != nil {
		return fmt.Errorf("%w: %v", ErrSimulationFailed, err)
	}

	return nil
}

func (r *Relayer) validateBasicBatchFields(msg *SignedBatchInstantExecute) error {
	if msg.Version != "1.0" {
		return ErrInvalidVersion
	}
	if msg.Type != "batch_instant_execute" {
		return ErrInvalidType
	}
	if len(msg.Signature) != 65 {
		return ErrInvalidSignature
	}
	if len(msg.Items) == 0 {
		return errors.New("empty batch items")
	}
	for _, item := range msg.Items {
		if len(item.Data) == 0 {
			return errors.New("empty calldata in batch item")
		}
	}
	return nil
}

func (r *Relayer) validateBasicFields(msg *SignedInstantExecute) error {
	if msg.Version != "1.0" {
		return ErrInvalidVersion
	}
	if msg.Type != "instant_execute" {
		return ErrInvalidType
	}
	if len(msg.Signature) != 65 {
		return ErrInvalidSignature
	}
	if len(msg.Data) == 0 {
		return errors.New("empty calldata")
	}
	return nil
}

func (r *Relayer) parseWeiAmount(amountStr string) (*big.Int, error) {
	// Treat empty string as zero (optional fee)
	if amountStr == "" {
		return big.NewInt(0), nil
	}

	amount, ok := new(big.Int).SetString(amountStr, 10)
	if !ok || amount.Sign() < 0 {
		return nil, ErrInvalidAmount
	}
	return amount, nil
}

// ValidateChannelAddress validates that a channel address is legitimate
func (r *Relayer) ValidateChannelAddress(ctx context.Context, channelAddr string) error {
	if r.ethClient == nil {
		return fmt.Errorf("ethereum client not connected")
	}

	// Parse address
	channel := common.HexToAddress(channelAddr)
	if channel == (common.Address{}) {
		return ErrInvalidChannel
	}

	// Check if it's a contract
	code, err := r.ethClient.CodeAt(ctx, channel, nil)
	if err != nil {
		return fmt.Errorf("failed to get channel code: %w", err)
	}

	if len(code) == 0 {
		return ErrNotAContract
	}

	// factory address is configured in the Relayer (r.factoryAddress)

	// If it's a minimal proxy, extract the implementation address from the proxy
	var implAddr common.Address
	if isMinimalProxy(code) {
		impl, err := r.getImplementationAddress(ctx, channel)
		if err != nil {
			return fmt.Errorf("failed to extract implementation from proxy: %w", err)
		}
		implAddr = impl
	} else {
		// Not a proxy: use the contract itself as implementation
		implAddr = channel
	}

	// Get the implementation address the factory expects
	factoryImpl, err := r.getFactoryImplementation(ctx)
	if err != nil {
		return fmt.Errorf("failed to get factory implementation: %w", err)
	}

	// Compare addresses first (fast path)
	if implAddr != factoryImpl {
		// For diagnostics, compute and log code hashes
		implCode, _ := r.ethClient.CodeAt(ctx, implAddr, nil)
		factoryCode, _ := r.ethClient.CodeAt(ctx, factoryImpl, nil)
		implHash := sha256.Sum256(implCode)
		factoryHash := sha256.Sum256(factoryCode)
		logrus.Infof("channel implementation address: %s", implAddr.Hex())
		logrus.Infof("factory implementation address: %s", factoryImpl.Hex())
		logrus.Infof("channel implementation hash: %s", hex.EncodeToString(implHash[:]))
		logrus.Infof("factory implementation hash: %s", hex.EncodeToString(factoryHash[:]))
		return ErrChannelCodeMismatch
	}

	return nil
}

// getImplementationCodeHash gets the implementation code hash from factory
func (r *Relayer) getImplementationCodeHash(ctx context.Context, factoryAddr common.Address) (string, error) {
	if r.ethClient == nil {
		return "", fmt.Errorf("ethereum client not connected")
	}

	// Get factory's implementation address and compute its code hash
	impl, err := r.getFactoryImplementation(ctx)
	if err != nil {
		return "", err
	}
	code, err := r.ethClient.CodeAt(ctx, impl, nil)
	if err != nil {
		return "", fmt.Errorf("failed to get implementation code: %w", err)
	}
	hash := sha256.Sum256(code)
	return hex.EncodeToString(hash[:]), nil
}

// ValidateChannelCreation validates a signed channel creation request
func (r *Relayer) ValidateChannelCreation(ctx context.Context, msg *SignedChannelCreation) error {
	if err := r.validateChannelCreationBasicFields(msg); err != nil {
		return err
	}

	if msg.ChainID != r.chainID.Uint64() {
		return ErrChainIDMismatch
	}

	if !common.IsHexAddress(msg.User) {
		return ErrInvalidAddress
	}

	// Verify EIP-712 signature
	if err := r.verifyChannelCreationSignature(msg); err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}

	return nil
}

// ValidateBatchChannelCreation validates a batch channel creation request
func (r *Relayer) ValidateBatchChannelCreation(ctx context.Context, msg *BatchChannelCreation) error {
	if err := r.validateBatchChannelCreationBasicFields(msg); err != nil {
		return err
	}

	if msg.ChainID != r.chainID.Uint64() {
		return ErrChainIDMismatch
	}

	if len(msg.Users) != len(msg.Nonces) || len(msg.Users) != len(msg.Signatures) {
		return errors.New("array length mismatch in batch request")
	}

	if len(msg.Users) == 0 || len(msg.Users) > 50 {
		return errors.New("batch size must be between 1 and 50")
	}

	for i, user := range msg.Users {
		if !common.IsHexAddress(user) {
			return fmt.Errorf("invalid user address at index %d: %s", i, user)
		}

		// Create individual message for signature verification
		individualMsg := &SignedChannelCreation{
			Version:   msg.Version,
			Type:      "channel_create",
			User:      user,
			Nonce:     msg.Nonces[i],
			Signature: msg.Signatures[i],
			ChainID:   msg.ChainID,
		}

		if err := r.verifyChannelCreationSignature(individualMsg); err != nil {
			return fmt.Errorf("signature verification failed for user %s at index %d: %w", user, i, err)
		}
	}

	return nil
}

func (r *Relayer) validateChannelCreationBasicFields(msg *SignedChannelCreation) error {
	if msg.Version != "1.0" {
		return ErrInvalidVersion
	}
	if msg.Type != "channel_create" {
		return ErrInvalidType
	}
	if len(msg.Signature) != 65 {
		return ErrInvalidSignature
	}
	return nil
}

func (r *Relayer) validateBatchChannelCreationBasicFields(msg *BatchChannelCreation) error {
	if msg.Version != "1.0" {
		return ErrInvalidVersion
	}
	if msg.Type != "batch_channel_create" {
		return ErrInvalidType
	}
	return nil
}

func (r *Relayer) verifyChannelCreationSignature(msg *SignedChannelCreation) error {
	// EIP-712 domain separator for InstantExecutionChannelFactory
	domainSeparator := r.getChannelCreationDomainSeparator()

	// Type hash for Deploy(address user,uint256 nonce)
	typeHash := crypto.Keccak256Hash([]byte("Deploy(address user,uint256 nonce)"))

	// Encode the message
	user := common.HexToAddress(msg.User)
	encodedData := crypto.Keccak256(abiEncode(typeHash, user, big.NewInt(int64(msg.Nonce))))

	// Create the digest
	digest := crypto.Keccak256Hash(
		[]byte("\x19\x01"),
		domainSeparator.Bytes(),
		encodedData,
	)

	// Recover signer
	sig := make([]byte, len(msg.Signature))
	copy(sig, msg.Signature)
	if sig[64] >= 27 {
		sig[64] -= 27
	}

	pubKey, err := crypto.SigToPub(digest.Bytes(), sig)
	if err != nil {
		return fmt.Errorf("failed to recover public key: %w", err)
	}

	signer := crypto.PubkeyToAddress(*pubKey)
	expectedSigner := common.HexToAddress(msg.User)

	if signer != expectedSigner {
		return ErrInvalidSignature
	}

	return nil
}

func (r *Relayer) getChannelCreationDomainSeparator() common.Hash {
	// Domain separator for InstantExecutionChannelFactory
	domainTypeHash := crypto.Keccak256Hash([]byte("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	nameHash := crypto.Keccak256Hash([]byte("InstantExecutionChannelFactory"))
	versionHash := crypto.Keccak256Hash([]byte("2"))

	chainId := r.chainID
	verifyingContract := r.factoryAddress

	domainData := abiEncode(
		domainTypeHash,
		nameHash,
		versionHash,
		chainId,
		verifyingContract,
	)

	return crypto.Keccak256Hash(domainData)
}

// abiEncode performs basic ABI encoding for the given arguments
func abiEncode(args ...interface{}) []byte {
	var data []byte
	for _, arg := range args {
		switch v := arg.(type) {
		case common.Hash:
			data = append(data, v.Bytes()...)
		case common.Address:
			data = append(data, common.LeftPadBytes(v.Bytes(), 32)...)
		case *big.Int:
			data = append(data, common.LeftPadBytes(v.Bytes(), 32)...)
		case []byte:
			data = append(data, v...)
		}
	}
	return data
}
