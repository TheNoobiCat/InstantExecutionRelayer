package messagepack

type SignedChannelCreation struct {
	Version      string `msgpack:"ver"`   // Protocol version "1.0"
	Type         string `msgpack:"typ"`   // "channel_create"
	User         string `msgpack:"usr"`   // User address (hex)
	Nonce        uint64 `msgpack:"nonce"` // Nonce for replay protection
	CaptchaToken string `msgpack:"cap"`   // Cloudflare captcha token
	Signature    []byte `msgpack:"sig"`   // EIP-712 signature
	ChainID      uint64 `msgpack:"chain"` // Chain ID
}

type BatchChannelCreation struct {
	Version      string   `msgpack:"ver"`    // Protocol version "1.0"
	Type         string   `msgpack:"typ"`    // "batch_channel_create"
	Users        []string `msgpack:"usrs"`   // Array of user addresses (hex)
	Nonces       []uint64 `msgpack:"nonces"` // Array of nonces
	CaptchaToken string   `msgpack:"cap"`    // Cloudflare captcha token
	Signatures   [][]byte `msgpack:"sigs"`   // Array of EIP-712 signatures
	ChainID      uint64   `msgpack:"chain"`  // Chain ID
}
