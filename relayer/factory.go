package relayer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
)

// Support both `CHANNEL_IMPLEMENTATION()` (used by this factory) and a
// legacy `implementation()` getter if present. We'll attempt to call
// CHANNEL_IMPLEMENTATION first and fall back to implementation.
var factoryABI = `[{"inputs": [{"type": "address", "name": "user"}],
	"name": "getUserChannels",
	"outputs": [{"type": "address[]", "name": ""}],
	"stateMutability": "view",
	"type": "function"
}, {"inputs": [], "name": "CHANNEL_IMPLEMENTATION", "outputs": [{"type": "address","name":""}], "stateMutability":"view","type":"function"}, {"inputs": [], "name": "implementation", "outputs": [{"type": "address","name":""}], "stateMutability":"view","type":"function"}]`

func (r *Relayer) ValidateChannelFactory(ctx context.Context, channelAddr string) error {
	if r.ethClient == nil {
		return fmt.Errorf("ethereum client not connected")
	}

	channel := common.HexToAddress(channelAddr)

	code, err := r.ethClient.CodeAt(ctx, channel, nil)
	if err != nil {
		return fmt.Errorf("failed to get code: %v", err)
	}
	if len(code) == 0 {
		return errors.New("address is not a contract")
	}

	if !isMinimalProxy(code) {
		return errors.New("not a minimal proxy contract")
	}

	implementation, err := r.getImplementationAddress(ctx, channel)
	if err != nil {
		return fmt.Errorf("failed to get implementation: %v", err)
	}

	factoryImplementation, err := r.getFactoryImplementation(ctx)
	if err != nil {
		return fmt.Errorf("failed to get factory implementation: %v", err)
	}

	if implementation != factoryImplementation {
		return errors.New("channel uses wrong implementation")
	}

	return nil
}

func isMinimalProxy(code []byte) bool {
	// EIP-1167 minimal proxy runtime bytecode layout (standard):
	// 0x363d3d373d3d3d363d73<20 bytes implementation>5af43d82803e903d91602b57fd5bf3
	prefix := common.FromHex("0x363d3d373d3d3d363d73")
	suffix := common.FromHex("0x5af43d82803e903d91602b57fd5bf3")

	// Look for prefix in the runtime code and ensure suffix follows the implementation address
	idx := bytes.Index(code, prefix)
	if idx == -1 {
		return false
	}

	implStart := idx + len(prefix)
	if implStart+20+len(suffix) > len(code) {
		return false
	}

	if !bytes.Equal(code[implStart+20:implStart+20+len(suffix)], suffix) {
		return false
	}

	return true
}

func (r *Relayer) getImplementationAddress(ctx context.Context, proxy common.Address) (common.Address, error) {
	if r.ethClient == nil {
		return common.Address{}, fmt.Errorf("ethereum client not connected")
	}

	code, err := r.ethClient.CodeAt(ctx, proxy, nil)
	if err != nil {
		return common.Address{}, err
	}

	// Find the implementation address inside the EIP-1167 proxy runtime code
	prefix := common.FromHex("0x363d3d373d3d3d363d73")
	idx := bytes.Index(code, prefix)
	if idx == -1 {
		return common.Address{}, errors.New("proxy prefix not found in code")
	}

	implStart := idx + len(prefix)
	if implStart+20 > len(code) {
		return common.Address{}, errors.New("invalid proxy code length")
	}

	implBytes := code[implStart : implStart+20]
	return common.BytesToAddress(implBytes), nil
}

func (r *Relayer) getFactoryImplementation(ctx context.Context) (common.Address, error) {
	if r.ethClient == nil {
		return common.Address{}, fmt.Errorf("ethereum client not connected")
	}

	// Use ABI call to fetch implementation() from factory contract
	parsed, err := abi.JSON(strings.NewReader(factoryABI))
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to parse factory ABI: %w", err)
	}
	bound := bind.NewBoundContract(r.factoryAddress, parsed, r.ethClient, r.ethClient, r.ethClient)

	// Helper to call a method and extract the first address return value
	callImpl := func(name string) (common.Address, error) {
		var results []interface{}
		if err := bound.Call(&bind.CallOpts{Context: ctx}, &results, name); err != nil {
			return common.Address{}, err
		}
		if len(results) == 0 {
			return common.Address{}, errors.New("no results from factory call")
		}
		// The returned value may be common.Address or a slice/typed value depending on ABI parsing
		switch v := results[0].(type) {
		case common.Address:
			return v, nil
		case *common.Address:
			return *v, nil
		case []byte:
			// sometimes addresses are returned as bytes
			if len(v) == 20 {
				return common.BytesToAddress(v), nil
			}
		case string:
			return common.HexToAddress(v), nil
		default:
			return common.Address{}, fmt.Errorf("unexpected factory return type: %T", v)
		}
		return common.Address{}, fmt.Errorf("unable to parse factory return type: %T", results[0])
	}

	// First attempt: CHANNEL_IMPLEMENTATION()
	if impl, err := callImpl("CHANNEL_IMPLEMENTATION"); err == nil {
		return impl, nil
	}

	// Fallback: implementation()
	if impl, err := callImpl("implementation"); err == nil {
		return impl, nil
	}

	return common.Address{}, fmt.Errorf("factory implementation call failed (tried CHANNEL_IMPLEMENTATION and implementation)")
}
