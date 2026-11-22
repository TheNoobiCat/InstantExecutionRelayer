package leaderboard

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	"github.com/sirupsen/logrus"
)

// Handler handles HTTP requests for leaderboard data
type Handler struct {
	db *LeaderboardDB
}

// NewHandler creates a new leaderboard HTTP handler
func NewHandler(db *LeaderboardDB) *Handler {
	return &Handler{db: db}
}

// Response structures
type LeaderboardResponse struct {
	Success bool               `json:"success"`
	Data    []LeaderboardEntry `json:"data,omitempty"`
	Error   string             `json:"error,omitempty"`
}

type UserStatsResponse struct {
	Success     bool    `json:"success"`
	UserAddress string  `json:"user_address,omitempty"`
	TradeCount  uint64  `json:"trade_count,omitempty"`
	Profit      float64 `json:"profit,omitempty"`
	Error       string  `json:"error,omitempty"`
}

// ServeHTTP handles the /leaderboard endpoint
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Debug endpoint: /leaderboard?debug=1
	if r.URL.Query().Get("debug") == "1" {
		h.handleDebugDump(w, r)
		return
	}

	// User stats: /leaderboard?user=0x...
	userAddr := r.URL.Query().Get("user")
	if userAddr != "" {
		h.handleUserStats(w, r, userAddr)
		return
	}

	// Leaderboard
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	sortStr := r.URL.Query().Get("sort")
	sortBy := SortByTradeCount
	if sortStr == "profit" {
		sortBy = SortByProfit
	}

	entries, err := h.db.GetTopAgentTraders(limit, sortBy)
	if err != nil {
		logrus.WithError(err).Error("Failed to get top agent traders")
		json.NewEncoder(w).Encode(LeaderboardResponse{
			Success: false,
			Error:   "Failed to retrieve leaderboard data",
		})
		return
	}

	json.NewEncoder(w).Encode(LeaderboardResponse{
		Success: true,
		Data:    entries,
	})
}

// handleUserStats handles requests for specific user statistics
func (h *Handler) handleUserStats(w http.ResponseWriter, r *http.Request, userAddr string) {
	count, err := h.db.GetUserAgentTrades(userAddr)
	if err != nil {
		logrus.WithError(err).WithField("user", userAddr).Error("Failed to get user stats")
		json.NewEncoder(w).Encode(UserStatsResponse{
			Success: false,
			Error:   "Failed to retrieve user statistics",
		})
		return
	}

	profit, err := h.db.CalculateUserProfit(userAddr)
	if err != nil {
		logrus.WithError(err).WithField("user", userAddr).Warn("Failed to calculate user profit")
		profit = 0
	}

	json.NewEncoder(w).Encode(UserStatsResponse{
		Success:     true,
		UserAddress: userAddr,
		TradeCount:  count,
		Profit:      profit,
	})
}

// handleDebugDump shows channels from factory + balances for all tracked users
func (h *Handler) handleDebugDump(w http.ResponseWriter, r *http.Request) {
	type channelInfo struct {
		Address string `json:"address"`
		Balance string `json:"balance_usdc"`
		Error   string `json:"error,omitempty"`
	}
	type userDebug struct {
		TradeCount uint64        `json:"trade_count"`
		Channels   []channelInfo `json:"channels"`
		TotalUSDC  string        `json:"total_usdc"`
		Profit     string        `json:"profit"`
	}

	result := map[string]interface{}{
		"client_configured": h.db.client != nil,
		"usdc_address":      h.db.usdcAddress.Hex(),
		"factory_address":   h.db.factoryAddress.Hex(),
	}

	// Get all users from the trades bucket
	entries, err := h.db.GetAllAgentTraders()
	if err != nil {
		result["error"] = err.Error()
		json.NewEncoder(w).Encode(result)
		return
	}

	users := map[string]userDebug{}
	for _, e := range entries {
		ud := userDebug{TradeCount: e.TradeCount}

		channels, err := h.db.GetUserChannelsFromChain(e.UserAddress)
		if err != nil {
			ud.Channels = []channelInfo{{Error: err.Error()}}
		} else {
			totalBalance := new(big.Int)
			for _, ch := range channels {
				ci := channelInfo{Address: ch.Hex()}
				bal, err := h.db.QueryBalance(ch)
				if err != nil {
					ci.Error = err.Error()
					ci.Balance = "0"
				} else {
					totalBalance.Add(totalBalance, bal)
					ci.Balance = formatUSDC(bal)
				}
				ud.Channels = append(ud.Channels, ci)
			}
			ud.TotalUSDC = formatUSDC(totalBalance)
			profitF, _ := new(big.Float).Quo(new(big.Float).SetInt(totalBalance), big.NewFloat(1e6)).Float64()
			ud.Profit = fmt.Sprintf("%.2f", profitF-1000.0)
		}

		users[e.UserAddress] = ud
	}

	result["users"] = users
	json.NewEncoder(w).Encode(result)
}

func formatUSDC(raw *big.Int) string {
	f := new(big.Float).Quo(new(big.Float).SetInt(raw), big.NewFloat(1e6))
	return f.Text('f', 2)
}
