package messagepack

type SignedSessionCreation struct {
	Version    string `msgpack:"ver"`   // Protocol version "1.0"
	Type       string `msgpack:"typ"`   // "session_create"
	Channel    string `msgpack:"ch"`    // Channel address (hex)
	SessionKey string `msgpack:"sk"`    // Session key address (hex)
	Duration   int64  `msgpack:"dur"`   // Duration in seconds
	Nonce      uint64 `msgpack:"nonce"` // Nonce
	Signature  []byte `msgpack:"sig"`   // Signature bytes (65 bytes)
	ChainID    uint64 `msgpack:"chain"` // Chain ID
}
