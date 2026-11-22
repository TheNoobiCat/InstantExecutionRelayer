package orderbook

import (
	"container/heap"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// TriggerType represents the type of triggerable order.
type TriggerType int

const (
	TriggerStopLoss TriggerType = iota
	TriggerTakeProfit
	TriggerLimitOrder
	TriggerLiquidation
)

func (t TriggerType) String() string {
	switch t {
	case TriggerStopLoss:
		return "StopLoss"
	case TriggerTakeProfit:
		return "TakeProfit"
	case TriggerLimitOrder:
		return "LimitOrder"
	case TriggerLiquidation:
		return "Liquidation"
	}
	return "Unknown"
}

// TriggerDirection indicates whether the order triggers when price goes above or below the trigger price.
type TriggerDirection int

const (
	// TriggerAbove: triggers when currentPrice >= triggerPrice (long TP, short SL, sell limit)
	TriggerAbove TriggerDirection = iota
	// TriggerBelow: triggers when currentPrice <= triggerPrice (long SL, short TP, buy limit)
	TriggerBelow
)

// TriggerableOrder represents an order that can be triggered by a price movement.
type TriggerableOrder struct {
	// Unique identifier: for TP/SL this is trader address, for limit orders this is orderId
	ID           string
	Type         TriggerType
	Direction    TriggerDirection
	TriggerPrice *big.Int // in 1e18 format (same as contract)
	// For TP/SL: trader address whose position to close
	Trader common.Address
	// For limit orders: the order ID (bytes32)
	OrderID [32]byte
	// Market info
	MarketSymbol  string
	TradingAddr   common.Address
	LimitBookAddr common.Address
	PythFeedID    string
	// Position details for TP/SL
	IsLong bool
	// Tracking
	CreatedAt   time.Time
	LastAttempt time.Time
	Attempts    int
	// Index in the heap (managed by heap.Interface)
	heapIndex int
}

// MarketOrderBook maintains two priority queues per market:
//   - UpperTriggers: min-heap by trigger price for orders that fire when price >= trigger
//     (long TP, short SL, sell limit orders)
//   - LowerTriggers: max-heap by trigger price for orders that fire when price <= trigger
//     (long SL, short TP, buy limit orders)
//
// This allows O(1) peek and O(log n) insert/remove for checking if any orders
// should trigger at a given price.
type MarketOrderBook struct {
	mu            sync.RWMutex
	MarketSymbol  string
	UpperTriggers *MinPriceHeap // triggers when price rises above; min-heap so we can check smallest trigger first
	LowerTriggers *MaxPriceHeap // triggers when price drops below; max-heap so we can check largest trigger first
	// Fast lookup by ID for O(1) deletion
	orderIndex map[string]*TriggerableOrder
}

// NewMarketOrderBook creates an empty order book for a market.
func NewMarketOrderBook(symbol string) *MarketOrderBook {
	upper := &MinPriceHeap{}
	lower := &MaxPriceHeap{}
	heap.Init(upper)
	heap.Init(lower)

	return &MarketOrderBook{
		MarketSymbol:  symbol,
		UpperTriggers: upper,
		LowerTriggers: lower,
		orderIndex:    make(map[string]*TriggerableOrder),
	}
}

// AddOrder inserts a triggerable order into the appropriate heap.
// O(log n) time complexity.
func (m *MarketOrderBook) AddOrder(order *TriggerableOrder) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove existing order with same ID if present
	if existing, ok := m.orderIndex[order.ID]; ok {
		m.removeOrderLocked(existing)
	}

	m.orderIndex[order.ID] = order

	switch order.Direction {
	case TriggerAbove:
		heap.Push(m.UpperTriggers, order)
	case TriggerBelow:
		heap.Push(m.LowerTriggers, order)
	}
}

// RemoveOrder removes an order by ID. O(log n) time complexity.
func (m *MarketOrderBook) RemoveOrder(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	order, ok := m.orderIndex[id]
	if !ok {
		return false
	}
	m.removeOrderLocked(order)
	return true
}

func (m *MarketOrderBook) removeOrderLocked(order *TriggerableOrder) {
	delete(m.orderIndex, order.ID)

	switch order.Direction {
	case TriggerAbove:
		if order.heapIndex >= 0 && order.heapIndex < m.UpperTriggers.Len() {
			heap.Remove(m.UpperTriggers, order.heapIndex)
		}
	case TriggerBelow:
		if order.heapIndex >= 0 && order.heapIndex < m.LowerTriggers.Len() {
			heap.Remove(m.LowerTriggers, order.heapIndex)
		}
	}
}

// GetTriggeredOrders returns all orders that should trigger at the given price.
// This is the hot path - called on every Pyth price update.
//
// For UpperTriggers (min-heap): pop all orders where triggerPrice <= currentPrice
// For LowerTriggers (max-heap): pop all orders where triggerPrice >= currentPrice
//
// Returns the triggered orders (removed from the book) without holding the lock
// during callback execution.
func (m *MarketOrderBook) GetTriggeredOrders(currentPrice *big.Int) []*TriggerableOrder {
	m.mu.Lock()
	defer m.mu.Unlock()

	var triggered []*TriggerableOrder

	// Check upper triggers: fire when currentPrice >= triggerPrice
	// Min-heap: peek the smallest trigger price. If currentPrice >= smallest, pop it.
	for m.UpperTriggers.Len() > 0 {
		top := (*m.UpperTriggers)[0]
		if currentPrice.Cmp(top.TriggerPrice) >= 0 {
			order := heap.Pop(m.UpperTriggers).(*TriggerableOrder)
			delete(m.orderIndex, order.ID)
			triggered = append(triggered, order)
		} else {
			break // No more upper triggers can fire
		}
	}

	// Check lower triggers: fire when currentPrice <= triggerPrice
	// Max-heap: peek the largest trigger price. If currentPrice <= largest, pop it.
	for m.LowerTriggers.Len() > 0 {
		top := (*m.LowerTriggers)[0]
		if currentPrice.Cmp(top.TriggerPrice) <= 0 {
			order := heap.Pop(m.LowerTriggers).(*TriggerableOrder)
			delete(m.orderIndex, order.ID)
			triggered = append(triggered, order)
		} else {
			break // No more lower triggers can fire
		}
	}

	return triggered
}

// Size returns the total number of orders across both heaps.
func (m *MarketOrderBook) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.orderIndex)
}

// HasOrder checks if an order exists by ID.
func (m *MarketOrderBook) HasOrder(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.orderIndex[id]
	return ok
}

// GetAllOrders returns a snapshot of all orders (for debugging/metrics).
func (m *MarketOrderBook) GetAllOrders() []*TriggerableOrder {
	m.mu.RLock()
	defer m.mu.RUnlock()
	orders := make([]*TriggerableOrder, 0, len(m.orderIndex))
	for _, o := range m.orderIndex {
		orders = append(orders, o)
	}
	return orders
}

// Clear removes all orders from the book.
func (m *MarketOrderBook) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	upper := &MinPriceHeap{}
	lower := &MaxPriceHeap{}
	heap.Init(upper)
	heap.Init(lower)
	m.UpperTriggers = upper
	m.LowerTriggers = lower
	m.orderIndex = make(map[string]*TriggerableOrder)
}

// --- Min-Heap (for upper triggers: orders that fire when price rises above) ---
// Smallest trigger price at top, so we can efficiently check "is current price >= smallest trigger?"

type MinPriceHeap []*TriggerableOrder

func (h MinPriceHeap) Len() int { return len(h) }

func (h MinPriceHeap) Less(i, j int) bool {
	return h[i].TriggerPrice.Cmp(h[j].TriggerPrice) < 0
}

func (h MinPriceHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *MinPriceHeap) Push(x interface{}) {
	order := x.(*TriggerableOrder)
	order.heapIndex = len(*h)
	*h = append(*h, order)
}

func (h *MinPriceHeap) Pop() interface{} {
	old := *h
	n := len(old)
	order := old[n-1]
	old[n-1] = nil
	order.heapIndex = -1
	*h = old[:n-1]
	return order
}

// --- Max-Heap (for lower triggers: orders that fire when price drops below) ---
// Largest trigger price at top, so we can efficiently check "is current price <= largest trigger?"

type MaxPriceHeap []*TriggerableOrder

func (h MaxPriceHeap) Len() int { return len(h) }

func (h MaxPriceHeap) Less(i, j int) bool {
	return h[i].TriggerPrice.Cmp(h[j].TriggerPrice) > 0 // reversed for max-heap
}

func (h MaxPriceHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *MaxPriceHeap) Push(x interface{}) {
	order := x.(*TriggerableOrder)
	order.heapIndex = len(*h)
	*h = append(*h, order)
}

func (h *MaxPriceHeap) Pop() interface{} {
	old := *h
	n := len(old)
	order := old[n-1]
	old[n-1] = nil
	order.heapIndex = -1
	*h = old[:n-1]
	return order
}
