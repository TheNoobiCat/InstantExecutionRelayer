package messagepack

type SignedInstantExecute struct {
	Version   string `msgpack:"v"`   // Protocol version "1.0"
	Type      string `msgpack:"t"`   // "instant_execute" etc
	Channel   string `msgpack:"c"`   // Channel address (hex)
	Nonce     uint64 `msgpack:"n"`   // Session key nonce
	Target    string `msgpack:"to"`  // Target contract address
	Value     string `msgpack:"val"` // ETH value in wei (string for big numbers) - FIXED
	Data      []byte `msgpack:"d"`   // Calldata
	Signature []byte `msgpack:"s"`   // EIP-712 signature
	Expires   int64  `msgpack:"e"`   // Unix timestamp
	ChainID   uint64 `msgpack:"cid"` // Chain ID for replay protection
}

// GasEstimationRequest is used to request gas estimation for session creation
type GasEstimationRequest struct {
	Version       string `msgpack:"v"`   // Protocol version "1.0"
	Type          string `msgpack:"t"`   // "gas_estimation"
	Channel       string `msgpack:"c"`   // Channel address (hex)
	SessionKey    string `msgpack:"sk"`  // Session key address (hex)
	Expiry        int64  `msgpack:"exp"` // Session expiry timestamp
	SpendingLimit string `msgpack:"sl"`  // Spending limit in wei (string)
	Nonce         uint64 `msgpack:"n"`   // Nonce
	Deadline      int64  `msgpack:"dl"`  // Signature deadline
	Signature     []byte `msgpack:"s"`   // EIP-712 signature (for validation)
	ChainID       uint64 `msgpack:"cid"` // Chain ID
}

type BatchItem struct {
	Target string `msgpack:"to"`  // Target contract address
	Value  string `msgpack:"val"` // ETH value in wei (string for big numbers)
	Data   []byte `msgpack:"d"`   // Calldata
}

type SignedBatchInstantExecute struct {
	Version   string      `msgpack:"v"`                   // Protocol version "1.0"
	Type      string      `msgpack:"t"`                   // "batch_instant_execute"
	Channel   string      `msgpack:"c"`                   // Channel address (hex)
	Items     []BatchItem `msgpack:"items"`               // Array of batch items
	Nonce     uint64      `msgpack:"n"`                   // Session key nonce
	Signature []byte      `msgpack:"s"`                   // EIP-712 signature
	Expires   int64       `msgpack:"e"`                   // Unix timestamp
	ChainID   uint64      `msgpack:"cid"`                 // Chain ID for replay protection
	ViaAgent  *bool       `msgpack:"via_agent,omitempty"` // Optional: true if executed by AI agent
}
