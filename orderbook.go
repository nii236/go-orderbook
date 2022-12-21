package orderbook

import (
	"errors"
	"fmt"
	"time"

	"github.com/edofic/go-ordmap/v2"
	"github.com/gofrs/uuid"
)

var ErrOrderExists = errors.New("order ID already exists")
var ErrNotImplemented = errors.New("not implemented")
var ErrNoLiquidity = errors.New("not enough open orders")
var ErrOrderNotExist = errors.New("order does not exist")
var ErrOrderQueueNotExist = errors.New("order queue does not exist")
var ErrEmptyOrder = errors.New("malformed order")
var ErrInvalidSide = errors.New("invalid side")

type OrderType string

const LimitBuy OrderType = "LIMIT_BUY"
const LimitSell OrderType = "LIMIT_SELL"
const MarketBuy OrderType = "MARKET_BUY"
const MarketSell OrderType = "MARKET_SELL"

type LimitNode struct {
	LimitPrice  uint64
	Size        uint64
	TotalVolume uint64
	Parent      *LimitNode
	LeftChild   *LimitNode
	RightChild  *LimitNode
	HeadOrder   *Order
	TailOrder   *Order
}
type CancelOrder struct {
	ID    uint64
	Type  OrderType
	Price uint64
}
type Order struct {
	ID        uint64
	Type      OrderType
	Quantity  uint64
	Price     uint64
	CreatedAt time.Time
}

type OrderQueue struct {
	Entries ordmap.NodeBuiltin[uint64, Order]
}

type PriceMap struct {
	Entries ordmap.NodeBuiltin[uint64, OrderQueue]
}

func NewPriceMap(entries []*Order) *PriceMap {
	pm := &PriceMap{Entries: ordmap.NewBuiltin[uint64, OrderQueue]()}
	for _, entry := range entries {
		oq, _ := pm.Entries.Get(entry.Price)
		oq.Entries = oq.Entries.Insert(entry.ID, *entry)
		pm.Entries = pm.Entries.Insert(entry.Price, oq)
	}
	return pm
}

type Orderbook struct {
	Symbol  string
	started bool

	bids   *PriceMap
	asks   *PriceMap
	add    chan (*Order)
	cancel chan (*CancelOrder)
	stop   chan (bool)
	errors chan (error)
}

func NewOrderbook(symbol string, bids *PriceMap, asks *PriceMap) *Orderbook {
	add := make(chan *Order)
	cancel := make(chan *CancelOrder)
	stop := make(chan bool)
	errors := make(chan error)
	return &Orderbook{
		Symbol: symbol,
		bids:   bids,
		asks:   asks,
		add:    add,
		cancel: cancel,
		stop:   stop,
		errors: errors,
	}
}

// Reset the orderbook, emptying out all bids and asks.
func (ob *Orderbook) Reset() {
	ob.add = make(chan *Order)
	ob.cancel = make(chan *CancelOrder)
	ob.errors = make(chan error)
	ob.asks = &PriceMap{}
	ob.bids = &PriceMap{}
}

func (ob *Orderbook) String() string {
	result := fmt.Sprintf("Orderbook [%s]\n", ob.Symbol)
	askQueue := ob.asks.Entries.IterateReverse()
	for askNode := askQueue; !askNode.Done(); askNode.Next() {
		result += fmt.Sprintf("%s %d ", "ASK", askNode.GetKey())
		orderQueue := askNode.GetValue().Entries.Iterate()
		for orderNode := orderQueue; !orderNode.Done(); orderNode.Next() {
			result += fmt.Sprintf("%d ", orderNode.GetValue().Quantity)
		}
		result += "\n"
	}
	result += "\n"
	bidQueue := ob.bids.Entries.Iterate()
	for bidNode := bidQueue; !bidNode.Done(); bidNode.Next() {
		result += fmt.Sprintf("%s %d ", "BID", bidNode.GetKey())
		orderQueue := bidNode.GetValue().Entries.Iterate()
		for orderNode := orderQueue; !orderNode.Done(); orderNode.Next() {
			result += fmt.Sprintf("%d ", orderNode.GetValue().Quantity)
		}
		result += "\n"
	}
	return result
}

func (ob *Orderbook) Stop() {
	if !ob.started {
		return
	}
	ob.stop <- true
}

// Run the service
func (ob *Orderbook) Run() {
	ob.started = true
	for {
		select {
		case <-ob.stop:
			fmt.Println("stopping")
			return
		case o := <-ob.add:
			switch o.Type {
			case LimitBuy:
				ob.errors <- ob.processLimitBuy(o)
			case LimitSell:
				highestBid, err := ob.highestBid()
				if err != nil && !errors.Is(err, ErrNoLiquidity) {
					ob.errors <- fmt.Errorf("process limit sell: %w", err)
					continue
				}
				if err == ErrNoLiquidity || o.Price > highestBid.Price {
					// No matching orders required
					askQueue, _ := ob.asks.Entries.Get(o.Price)
					askQueue.Entries = askQueue.Entries.Insert(o.ID, *o)
					ob.asks.Entries = ob.asks.Entries.Insert(o.Price, askQueue)
					ob.errors <- nil
					continue
				}

				// Matching orders needed
				result, err := ob.calculateLimitSell(o)
				if err != nil {
					ob.errors <- err
					continue
				}
				if result.Remaining != nil && result.Remaining.Quantity > 0 {
					err = ob.orderUpsert(*result.Remaining)
					if err != nil {
						ob.errors <- err
						continue
					}

				}
				for _, filledOrder := range result.Filled {
					err = ob.orderRemove(filledOrder)
					if err != nil {
						ob.errors <- err
						continue
					}

				}
				if result.Partial != nil {
					err = ob.orderUpsert(*result.Partial)
					if err != nil {
						ob.errors <- err
						continue
					}
				}

				ob.errors <- err
				continue
			case MarketBuy:
				ob.errors <- ob.processMarketBuy(o)
			case MarketSell:
				ob.errors <- ob.processMarketSell(o)
			default:
				continue
			}
		case o := <-ob.cancel:
			ob.errors <- ob.processCancel(o)
		}
	}

}

func (ob *Orderbook) Add(o *Order) error {
	if o == nil {
		return ErrEmptyOrder
	}
	if o.Type == LimitBuy {
		bidQueue, _ := ob.bids.Entries.Get(o.Price)
		_, exists := bidQueue.Entries.Get(o.ID)
		if exists {
			return ErrOrderExists
		}
	}
	ob.add <- o
	err := <-ob.errors
	return err
}
func (ob *Orderbook) Cancel(o *CancelOrder) error {
	if o == nil {
		return ErrEmptyOrder
	}
	ob.cancel <- o
	return <-ob.errors
}

func (ob *Orderbook) Bids() []Order {
	result := []Order{}
	bidQueue := ob.bids.Entries.Iterate()
	for i := bidQueue; !i.Done(); i.Next() {
		orderQueue := i.GetValue().Entries.Iterate()
		for j := orderQueue; !j.Done(); j.Next() {
			fmt.Println(j.GetKey(), j.GetValue())
			result = append(result, j.GetValue())
		}
	}
	return result
}

func (ob *Orderbook) Asks() []Order {
	result := []Order{}
	askQueue := ob.asks.Entries
	for i := askQueue.Iterate(); !i.Done(); i.Next() {
		orderQueue := i.GetValue().Entries
		for j := orderQueue.Iterate(); !j.Done(); j.Next() {
			result = append(result, j.GetValue())
		}
	}
	return result
}

func (ob *Orderbook) processLimitBuy(o *Order) error {
	oq, exists := ob.bids.Entries.Get(o.Price)
	if !exists {
		return ErrOrderQueueNotExist
	}
	oq.Entries.Insert(o.ID, *o)
	return nil
}

type LimitSellResult struct {
	Remaining *Order
	Partial   *Order
	Filled    []Order
	Trades    []Trade
}

func (ob *Orderbook) calculateLimitSell(o *Order) (*LimitSellResult, error) {
	remainingQuantity := o.Quantity
	filled := []Order{}
	var partial *Order
	// Matching orders required
	// Calculate full matches
	bidQueue := ob.bids.Entries.IterateReverse()
	for bidNode := bidQueue; !bidNode.Done(); bidNode.Next() {
		// Get orders within a single bid/ask matching price
		bidPrice := bidNode.GetKey()
		if o.Price > bidPrice {
			break
		}
		fmt.Println("filling price", bidPrice)
		if remainingQuantity == 0 {
			break
		}
		orderQueue := bidNode.GetValue().Entries.Iterate()
		for orderNode := orderQueue; !orderNode.Done(); orderNode.Next() {
			// Match orders
			if remainingQuantity == 0 {
				break
			}
			matchedOrder := orderNode.GetValue()
			fmt.Printf("matching order %d, qty %d price %d:", matchedOrder.ID, matchedOrder.Quantity, matchedOrder.Price)
			if remainingQuantity < matchedOrder.Quantity {
				partial = &Order{
					ID:        matchedOrder.ID,
					Type:      matchedOrder.Type,
					Quantity:  matchedOrder.Quantity - remainingQuantity,
					Price:     matchedOrder.Price,
					CreatedAt: matchedOrder.CreatedAt,
				}
				remainingQuantity -= matchedOrder.Quantity
			}
			if remainingQuantity > matchedOrder.Quantity {
				filled = append(filled, matchedOrder)
				remainingQuantity -= matchedOrder.Quantity
			}

			filled = append(filled, matchedOrder)
			remainingQuantity = 0
		}
	}
	var remainingOrder *Order
	if remainingQuantity > 0 {
		remainingOrder = &Order{
			ID:        o.ID,
			Type:      o.Type,
			Quantity:  remainingQuantity,
			Price:     o.Price,
			CreatedAt: o.CreatedAt,
		}
	}

	trades := []Trade{}
	for _, filledOrder := range filled {
		trades = append(trades, Trade{AskOrderID: o.ID, BidOrderID: filledOrder.ID, Amount: filledOrder.Quantity, Price: filledOrder.Price})
	}
	if partial != nil {
		trades = append(trades, Trade{ID: uuid.Must(uuid.NewV4()), AskOrderID: o.ID, BidOrderID: partial.ID, Amount: partial.Quantity, Price: partial.Price})
	}

	return &LimitSellResult{
		Remaining: remainingOrder,
		Partial:   partial,
		Filled:    filled,
		Trades:    trades,
	}, nil
}

type Trade struct {
	ID         uuid.UUID
	BidOrderID uint64
	AskOrderID uint64
	Price      uint64
	Amount     uint64
}

func (ob *Orderbook) orderUpsert(o Order) error {
	switch o.Type {
	case LimitBuy:
		oq, exists := ob.bids.Entries.Get(o.Price)
		if !exists {
			ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		}
		_, exists = oq.Entries.Get(o.ID)
		if !exists {
			return fmt.Errorf("get order: %w", ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		return nil
	case LimitSell:
		oq, exists := ob.asks.Entries.Get(o.Price)
		if !exists {
			ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		}
		_, exists = oq.Entries.Get(o.ID)
		if !exists {
			return fmt.Errorf("get order: %w", ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		return nil
	default:
		return ErrInvalidSide
	}
}
func (ob *Orderbook) orderRemove(o Order) error {
	switch o.Type {
	case LimitBuy:
		oq, exists := ob.bids.Entries.Get(o.Price)
		if !exists {
			return fmt.Errorf("get orderqueue: %w", ErrOrderNotExist)
		}
		_, exists = oq.Entries.Get(o.ID)
		if !exists {
			return fmt.Errorf("get order: %w", ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Remove(o.ID)
		return nil
	case LimitSell:
		oq, exists := ob.asks.Entries.Get(o.Price)
		if !exists {
			return fmt.Errorf("get orderqueue: %w", ErrOrderNotExist)
		}
		_, exists = oq.Entries.Get(o.ID)
		if !exists {
			return fmt.Errorf("get order: %w", ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Remove(o.ID)
		return nil
	default:
		return ErrInvalidSide
	}
}

func (ob *Orderbook) highestBid() (Order, error) {
	if ob.bids.Entries.Len() <= 0 {
		return Order{}, ErrNoLiquidity
	}
	return ob.bids.Entries.Max().V.Entries.Min().V, nil
}
func (ob *Orderbook) lowestAsk() (Order, error) {
	if ob.bids.Entries.Len() <= 0 {
		return Order{}, ErrNoLiquidity
	}
	return ob.bids.Entries.Max().V.Entries.Min().V, nil
}
func (ob *Orderbook) processCancel(o *CancelOrder) error {
	var oq OrderQueue
	exists := false
	if o.Type == LimitBuy {
		oq, exists = ob.bids.Entries.Get(o.Price)
		if !exists {
			ob.errors <- ErrOrderNotExist
		}
	} else {
		oq, exists = ob.asks.Entries.Get(o.Price)
		if !exists {
			ob.errors <- ErrOrderNotExist
		}
	}

	_, exists = oq.Entries.Get(o.ID)
	if !exists {
		ob.errors <- ErrOrderNotExist
	}

	oq.Entries = oq.Entries.Remove(o.ID)
	if o.Type == LimitBuy {
		if oq.Entries.Len() == 0 {
			// Empty order queue, remove completely
			ob.bids.Entries = ob.bids.Entries.Remove(o.Price)
			return nil
		}
		ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
	} else {
		if oq.Entries.Len() == 0 {
			// Empty order queue, remove completely
			ob.asks.Entries = ob.asks.Entries.Remove(o.Price)
			return nil
		}
		ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
	}
	return nil
}

func (ob *Orderbook) processMarketSell(o *Order) error {
	return ErrNotImplemented
}
func (ob *Orderbook) processMarketBuy(o *Order) error {
	return ErrNotImplemented
}
