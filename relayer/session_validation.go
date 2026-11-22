package relayer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	ErrInvalidSessionKey = errors.New("invalid session key")
	ErrInvalidExpiry     = errors.New("invalid expiry time")
)

func (r *Relayer) ValidateSessionCreation(ctx context.Context, msg *SignedSessionCreation) error {
	if err := r.validateSessionBasicFields(msg); err != nil {
		return err
	}

	// Validate channel address first
	// if err := r.ValidateChannelAddress(ctx, msg.Channel); err != nil {
	// 	return fmt.Errorf("channel validation failed: %w", err)
	// }

	if !common.IsHexAddress(msg.Channel) || !common.IsHexAddress(msg.SessionKey) {
		return ErrInvalidAddress
	}

	expiry := time.Now().Unix() + msg.Duration
	if expiry <= time.Now().Unix()+60 {
		return ErrInvalidExpiry
	}
	if expiry > time.Now().Add(30*24*time.Hour).Unix() {
		return errors.New("session expiry too far in future")
	}

	if msg.ChainID != r.chainID.Uint64() {
		return ErrChainIDMismatch
	}

	// Skip signature verification - rely on simulation only
	if err := r.verifySessionCreationSignature(ctx, msg); err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}

	if err := r.simulateSessionCreation(ctx, msg); err != nil {
		return fmt.Errorf("session creation simulation failed: %v", err)
	}

	return nil
}

func (r *Relayer) validateSessionBasicFields(msg *SignedSessionCreation) error {
	if msg.Version != "1.0" {
		return ErrInvalidVersion
	}
	if msg.Type != "session_create" {
		return ErrInvalidType
	}
	if len(msg.Signature) != 65 {
		return ErrInvalidSignature
	}
	if msg.SessionKey == msg.Channel {
		return errors.New("session key cannot be channel address")
	}
	return nil
}

func (r *Relayer) getChannelOwner(ctx context.Context, channelAddr string) (common.Address, error) {
	channel := common.HexToAddress(channelAddr)

	// ABI for owner() function
	ownerABI := `[{"inputs":[],"name":"owner","outputs":[{"type":"address","name":""}],"stateMutability":"view","type":"function"}]`

	parsed, err := abi.JSON(strings.NewReader(ownerABI))
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to parse owner ABI: %w", err)
	}

	bound := bind.NewBoundContract(channel, parsed, r.ethClient, r.ethClient, r.ethClient)

	var results []interface{}
	if err := bound.Call(&bind.CallOpts{Context: ctx}, &results, "owner"); err != nil {
		return common.Address{}, fmt.Errorf("failed to call owner: %w", err)
	}

	if len(results) == 0 {
		return common.Address{}, errors.New("no results from owner call")
	}

	if addr, ok := results[0].(common.Address); ok {
		return addr, nil
	}

	return common.Address{}, fmt.Errorf("unexpected owner return type: %T", results[0])
}

func (r *Relayer) verifySessionCreationSignature(ctx context.Context, msg *SignedSessionCreation) error {
	// Get the channel owner
	owner, err := r.getChannelOwner(ctx, msg.Channel)
	if err != nil {
		return fmt.Errorf("failed to get channel owner: %w", err)
	}

	// Domain separator for InstantExecutionChannel
	domainTypeHash := crypto.Keccak256Hash([]byte("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	nameHash := crypto.Keccak256Hash([]byte("InstantExecutionChannel"))
	versionHash := crypto.Keccak256Hash([]byte("2"))
	chainId := r.chainID
	verifyingContract := common.HexToAddress(msg.Channel)

	domainData := abiEncode(
		domainTypeHash,
		nameHash,
		versionHash,
		chainId,
		verifyingContract,
	)
	domainSeparator := crypto.Keccak256Hash(domainData)

	// Type hash for Reconnect(address sessionKey,uint256 duration,uint256 nonce)
	typeHash := crypto.Keccak256Hash([]byte("Reconnect(address sessionKey,uint256 duration,uint256 nonce)"))

	// Use duration from message
	duration := msg.Duration
	if duration <= 0 {
		return fmt.Errorf("invalid duration: %d", duration)
	}

	// Encode the message
	sessionKey := common.HexToAddress(msg.SessionKey)
	encodedData := crypto.Keccak256(abiEncode(typeHash, sessionKey, big.NewInt(duration), big.NewInt(int64(msg.Nonce))))

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

	expectedSigner := owner
	if signer != expectedSigner {
		return ErrInvalidSignature
	}

	return nil
}
