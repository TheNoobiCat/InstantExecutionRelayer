package sync

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// GoldskyClient queries the BrokerFi subgraph for active limit orders.
type GoldskyClient struct {
	url        string
	httpClient *http.Client
}

// NewGoldskyClient creates a GraphQL client for the Goldsky subgraph.
func NewGoldskyClient(url string) *GoldskyClient {
	return &GoldskyClient{
		url: url,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// LimitOrderData represents a limit order from the subgraph.
type LimitOrderData struct {
	ID           string `json:"id"`
	OrderID      string `json:"orderId"`
	Trader       string `json:"trader"`
	Collateral   string `json:"collateral"`
	Size         string `json:"size"`
	IsLong       bool   `json:"isLong"`
	TriggerPrice string `json:"triggerPrice"`
	Market       struct {
		ID string `json:"id"`
	} `json:"market"`
}

type goldskyResponse struct {
	Data struct {
		LimitOrders []LimitOrderData `json:"limitOrders"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// FetchActiveLimitOrders queries the subgraph for all active limit orders for a given market.
func (g *GoldskyClient) FetchActiveLimitOrders(ctx context.Context, marketLOB common.Address) ([]LimitOrderData, error) {
	query := fmt.Sprintf(`{
		limitOrders(
			where: {
				market: "%s"
				status: ACTIVE
			}
			first: 1000
		) {
			id
			orderId
			trader
			collateral
			size
			isLong
			triggerPrice
			market { id }
		}
	}`, marketLOB.Hex())

	reqBody := map[string]string{"query": query}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", g.url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var result goldskyResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("GraphQL error: %s", result.Errors[0].Message)
	}

	return result.Data.LimitOrders, nil
}

// ParseOrderIDBytes converts a hex order ID from the subgraph (e.g. "0xd9cf...") to [32]byte.
func ParseOrderIDBytes(orderIDStr string) ([32]byte, error) {
	var id [32]byte
	s := strings.TrimPrefix(orderIDStr, "0x")
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("invalid hex order ID %q: %w", orderIDStr, err)
	}
	if len(b) != 32 {
		return id, fmt.Errorf("order ID %q is %d bytes, expected 32", orderIDStr, len(b))
	}
	copy(id[:], b)
	return id, nil
}
