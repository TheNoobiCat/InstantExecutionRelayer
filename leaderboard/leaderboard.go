package leaderboard

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

var (
	// AgentTradesBucket stores agent trade counts per user
	AgentTradesBucket = []byte("AgentTrades")
)

// LeaderboardDB wraps a bbolt database for leaderboard tracking
type LeaderboardDB struct {
	db             *bolt.DB
	client         *ethclient.Client
	usdcAddress    common.Address
	factoryAddress common.Address
}

// LeaderboardEntry represents a single leaderboard entry
type LeaderboardEntry struct {
	UserAddress string  `json:"user_address"`
	TradeCount  uint64  `json:"trade_count"`
	Profit      float64 `json:"profit"`
	Rank        int     `json:"rank"`
}

// Open opens or creates the leaderboard database
func Open(path string) (*LeaderboardDB, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open leaderboard db: %w", err)
	}

	// Create buckets if they don't exist
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(AgentTradesBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create buckets: %w", err)
	}

	return &LeaderboardDB{db: db}, nil
}

// SetClient sets the ethereum client, USDC address, and factory address for on-chain queries
func (l *LeaderboardDB) SetClient(client *ethclient.Client, usdcAddress, factoryAddress common.Address) {
	l.client = client
	l.usdcAddress = usdcAddress
	l.factoryAddress = factoryAddress
}

// Close closes the database
func (l *LeaderboardDB) Close() error {
	if l.db != nil {
		return l.db.Close()
	}
	return nil
}

// IncrementAgentTrades increments the agent trade count for a user
// Returns the new trade count
func (l *LeaderboardDB) IncrementAgentTrades(userAddress string) (uint64, error) {
	userAddress = strings.ToLower(userAddress)

	var newCount uint64
	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(AgentTradesBucket)

		currentBytes := b.Get([]byte(userAddress))
		var currentCount uint64
		if currentBytes != nil {
			currentCount = binary.BigEndian.Uint64(currentBytes)
		}

		newCount = currentCount + 1

		countBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(countBytes, newCount)

		return b.Put([]byte(userAddress), countBytes)
	})

	return newCount, err
}

// ABI fragments
const (
	erc20BalanceOfABI     = `[{"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`
	factoryGetChannelsABI = `[{"inputs":[{"name":"user","type":"address"}],"name":"getUserChannels","outputs":[{"name":"","type":"address[]"}],"stateMutability":"view","type":"function"}]`
)

// GetUserChannelsFromChain queries the factory contract to get all channel addresses for a user
func (l *LeaderboardDB) GetUserChannelsFromChain(userAddress string) ([]common.Address, error) {
	if l.client == nil || l.factoryAddress == (common.Address{}) {
		return nil, fmt.Errorf("client or factory address not configured")
	}

	parsed, err := abi.JSON(strings.NewReader(factoryGetChannelsABI))
	if err != nil {
		return nil, fmt.Errorf("parse factory ABI: %w", err)
	}

	bound := bind.NewBoundContract(l.factoryAddress, parsed, l.client, l.client, l.client)
	var results []interface{}
	err = bound.Call(&bind.CallOpts{Context: context.Background()}, &results, "getUserChannels", common.HexToAddress(userAddress))
	if err != nil {
		return nil, fmt.Errorf("getUserChannels call failed: %w", err)
	}

	if len(results) == 0 {
		return nil, nil
	}

	channels, ok := results[0].([]common.Address)
	if !ok {
		return nil, fmt.Errorf("unexpected result type from getUserChannels: %T", results[0])
	}

	return channels, nil
}

// QueryBalance queries balanceOf on the USDC contract for a given address
func (l *LeaderboardDB) QueryBalance(addr common.Address) (*big.Int, error) {
	parsed, err := abi.JSON(strings.NewReader(erc20BalanceOfABI))
	if err != nil {
		return nil, fmt.Errorf("parse ABI: %w", err)
	}
	bound := bind.NewBoundContract(l.usdcAddress, parsed, l.client, l.client, l.client)
	var results []interface{}
	if err := bound.Call(&bind.CallOpts{Context: context.Background()}, &results, "balanceOf", addr); err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return big.NewInt(0), nil
	}
	if v, ok := results[0].(*big.Int); ok {
		return v, nil
	}
	return big.NewInt(0), fmt.Errorf("unexpected type: %T", results[0])
}

// CalculateUserProfit queries the factory for the user's channels,
// sums USDC balanceOf across all of them, and subtracts 1000 (airdrop).
func (l *LeaderboardDB) CalculateUserProfit(userAddress string) (float64, error) {
	if l.client == nil || l.usdcAddress == (common.Address{}) || l.factoryAddress == (common.Address{}) {
		return 0, fmt.Errorf("not configured: client=%v usdc=%s factory=%s",
			l.client != nil, l.usdcAddress.Hex(), l.factoryAddress.Hex())
	}

	channels, err := l.GetUserChannelsFromChain(userAddress)
	if err != nil {
		return 0, fmt.Errorf("getUserChannels failed for %s: %w", userAddress, err)
	}

	if len(channels) == 0 {
		return -1000, nil
	}

	totalBalance := new(big.Int)
	for _, ch := range channels {
		bal, err := l.QueryBalance(ch)
		if err != nil {
			continue
		}
		totalBalance.Add(totalBalance, bal)
	}

	// USDC has 6 decimals
	balanceFloat, _ := new(big.Float).Quo(
		new(big.Float).SetInt(totalBalance),
		big.NewFloat(1e6),
	).Float64()

	return balanceFloat - 1000.0, nil
}

// GetUserAgentTrades gets the agent trade count for a specific user
func (l *LeaderboardDB) GetUserAgentTrades(userAddress string) (uint64, error) {
	userAddress = strings.ToLower(userAddress)

	var count uint64
	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(AgentTradesBucket)
		countBytes := b.Get([]byte(userAddress))
		if countBytes != nil {
			count = binary.BigEndian.Uint64(countBytes)
		}
		return nil
	})

	return count, err
}

// SortBy defines the sorting criteria
type SortBy string

const (
	SortByTradeCount SortBy = "trades"
	SortByProfit     SortBy = "profit"
)

// GetTopAgentTraders returns the top N users sorted by the specified criteria
func (l *LeaderboardDB) GetTopAgentTraders(limit int, sortBy SortBy) ([]LeaderboardEntry, error) {
	// First collect all users and trade counts from the DB
	type userTrade struct {
		addr  string
		count uint64
	}
	var users []userTrade

	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(AgentTradesBucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil {
				continue
			}
			users = append(users, userTrade{
				addr:  string(k),
				count: binary.BigEndian.Uint64(v),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Now calculate profit for each user (outside the bbolt tx)
	entries := make([]LeaderboardEntry, 0, len(users))
	for _, u := range users {
		profit, err := l.CalculateUserProfit(u.addr)
		if err != nil {
			logrus.WithError(err).WithField("user", u.addr).Error("Failed to calculate profit")
			profit = 0
		}

		entries = append(entries, LeaderboardEntry{
			UserAddress: u.addr,
			TradeCount:  u.count,
			Profit:      profit,
		})
	}

	// Sort
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			shouldSwap := false
			if sortBy == SortByProfit {
				shouldSwap = entries[j].Profit > entries[i].Profit
			} else {
				shouldSwap = entries[j].TradeCount > entries[i].TradeCount
			}
			if shouldSwap {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	// Assign ranks and limit
	for i := range entries {
		entries[i].Rank = i + 1
	}
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// GetAllAgentTraders returns all users with agent trades sorted by trade count
func (l *LeaderboardDB) GetAllAgentTraders() ([]LeaderboardEntry, error) {
	return l.GetTopAgentTraders(0, SortByTradeCount)
}
