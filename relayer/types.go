package relayer

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type SignedInstantExecute struct {
	Version   string `msgpack:"v"`
	Type      string `msgpack:"t"`
	Channel   string `msgpack:"c"`
	Nonce     uint64 `msgpack:"n"`
	Target    string `msgpack:"to"`
	Value     string `msgpack:"val"`
	Data      []byte `msgpack:"d"`
	Signature []byte `msgpack:"s"`
	Expires   int64  `msgpack:"e"`
	ChainID   uint64 `msgpack:"cid"`
}

type BatchItem struct {
	Target string `msgpack:"to"`
	Value  string `msgpack:"val"`
	Data   []byte `msgpack:"d"`
}

type SignedBatchInstantExecute struct {
	Version   string      `msgpack:"v"`
	Type      string      `msgpack:"t"`
	Channel   string      `msgpack:"c"`
	Items     []BatchItem `msgpack:"items"`
	Nonce     uint64      `msgpack:"n"`
	Signature []byte      `msgpack:"s"`
	Expires   int64       `msgpack:"e"`
	ChainID   uint64      `msgpack:"cid"`
}

type SignedSessionCreation struct {
	Version    string `msgpack:"ver"`
	Type       string `msgpack:"typ"`
	Channel    string `msgpack:"ch"`
	SessionKey string `msgpack:"sk"`
	Duration   int64  `msgpack:"dur"`
	Nonce      uint64 `msgpack:"nonce"`
	Signature  []byte `msgpack:"sig"`
	ChainID    uint64 `msgpack:"chain"`
}

type SignedChannelCreation struct {
	Version      string `msgpack:"ver"`
	Type         string `msgpack:"typ"`
	User         string `msgpack:"usr"`
	Nonce        uint64 `msgpack:"nonce"`
	CaptchaToken string `msgpack:"cap"`
	Signature    []byte `msgpack:"sig"`
	ChainID      uint64 `msgpack:"chain"`
}

type BatchChannelCreation struct {
	Version      string   `msgpack:"ver"`
	Type         string   `msgpack:"typ"`
	Users        []string `msgpack:"usrs"`
	Nonces       []uint64 `msgpack:"nonces"`
	CaptchaToken string   `msgpack:"cap"`
	Signatures   [][]byte `msgpack:"sigs"`
	ChainID      uint64   `msgpack:"chain"`
}

type SimulationResult struct {
	Success    bool
	ReturnData []byte
	GasUsed    uint64
	Error      string
}

type SessionCreationResult struct {
	Success bool
	TxHash  string
	Error   string
}

type ChannelCreationResult struct {
	Success bool
	TxHash  string
	Channel string
	Error   string
}

type BatchChannelCreationResult struct {
	Success  bool
	TxHash   string
	Channels []string
	Error    string
}

type WSSender interface {
	SendJSON(typ string, payload interface{}) error
}

type Relayer struct {
	ethClient          *ethclient.Client
	chainID            *big.Int
	factoryAddress     common.Address
	privateKey         *ecdsa.PrivateKey
	transactor         *bind.TransactOpts
	batchManager       *BatchManager
	PendingConnections map[uint64]WSSender
	leaderboardDB      interface{} // Will be *leaderboard.LeaderboardDB
}

func NewRelayer(client *ethclient.Client, chainID *big.Int, factory common.Address) *Relayer {
	return &Relayer{
		ethClient:          client,
		chainID:            chainID,
		factoryAddress:     factory,
		privateKey:         nil, // Will be set when configured
		transactor:         nil, // Will be set when private key is configured
		PendingConnections: make(map[uint64]WSSender),
		leaderboardDB:      nil, // Will be set when leaderboard is enabled
	}
}

// SetLeaderboardDB sets the leaderboard database
func (r *Relayer) SetLeaderboardDB(ldb interface{}) {
	r.leaderboardDB = ldb
}

// GetLeaderboardDB returns the leaderboard database (typed as interface to avoid import cycles)
func (r *Relayer) GetLeaderboardDB() interface{} {
	return r.leaderboardDB
}

// ExtractChannelOwner extracts the owner address from a channel address
// This queries the channel contract's owner() function to get the actual wallet address
func ExtractChannelOwner(client *ethclient.Client, channelAddress string) string {
	// Channel ABI with owner() view function
	ownerABI := `[{"inputs": [], "name": "owner", "outputs": [{"type": "address","name":""}], "stateMutability":"view","type":"function"}]`

	// Parse the channel address
	channel := common.HexToAddress(channelAddress)

	// Parse the ABI
	parsed, err := abi.JSON(strings.NewReader(ownerABI))
	if err != nil {
		// Fallback to channel address if ABI parsing fails
		return channelAddress
	}

	// Create bound contract
	bound := bind.NewBoundContract(channel, parsed, client, client, client)

	// Call owner() function
	var results []interface{}
	ctx := context.Background()
	if err := bound.Call(&bind.CallOpts{Context: ctx}, &results, "owner"); err != nil {
		// Fallback to channel address if contract call fails
		return channelAddress
	}

	// Extract owner address from results
	if len(results) == 0 {
		return channelAddress
	}

	switch v := results[0].(type) {
	case common.Address:
		return v.Hex()
	case *common.Address:
		return v.Hex()
	case []byte:
		if len(v) == 20 {
			return common.BytesToAddress(v).Hex()
		}
	case string:
		return common.HexToAddress(v).Hex()
	default:
		// Fallback to channel address if type is unexpected
		return channelAddress
	}

	return channelAddress
}

// SetWallet configures the relayer's wallet with a private key
func (r *Relayer) SetWallet(privateKey *ecdsa.PrivateKey) error {
	r.privateKey = privateKey

	// Create transactor options
	transactor, err := bind.NewKeyedTransactorWithChainID(privateKey, r.chainID)
	if err != nil {
		return err
	}

	r.transactor = transactor
	return nil
}

// AddChannelCreationRequest adds a channel creation request to the batch queue
func (r *Relayer) AddChannelCreationRequest(req *SignedChannelCreation) {
	if r.batchManager == nil {
		r.batchManager = NewBatchManager(r, r.privateKey, 16*time.Second, 50)
	}
	r.batchManager.AddRequest(req)
}
