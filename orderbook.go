package orderbook

import (
	"context"
	"errors"
	"fmt"
	"math"
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
var ErrNoError = errors.New("no error collected, no rollback needed")

type OrderType string

const Bid OrderType = "BUY"
const Ask OrderType = "SELL"

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
	// Size    uint64
	Entries ordmap.NodeBuiltin[uint64, Order]
}

type PriceMap struct {
	Size    uint64
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
	Symbol string
	bids   *PriceMap
	asks   *PriceMap
}

type RequestType string

const RequestCancel RequestType = "REQUEST_CANCEL"
const RequestAdd RequestType = "REQUEST_ADD"

type Request struct {
	Type  RequestType
	Order *Order
}

func NewOrderbook(symbol string) *Orderbook {
	return &Orderbook{
		Symbol: symbol,
		bids:   NewPriceMap([]*Order{}),
		asks:   NewPriceMap([]*Order{}),
	}
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

func (ob *Orderbook) processLimitSell(ctx context.Context, o *Order) ([]Trade, error) {
	highestBid, err := ob.highestBid()
	if err != nil && !errors.Is(err, ErrNoLiquidity) {
		WithError(ctx)
		return nil, fmt.Errorf("process limit sell: %w", err)
	}
	if err == ErrNoLiquidity || o.Price > highestBid.Price {
		// No matching orders required
		err = ob.orderInsert(ctx, *o)
		if err != nil && !errors.Is(err, ErrNoLiquidity) {
			WithError(ctx)
			return nil, fmt.Errorf("process limit sell: %w", err)
		}
		return []Trade{}, nil
	}

	// Matching orders required
	result, err := ob.calculateLimitSell(o)
	if err != nil {
		WithError(ctx)
		return nil, err
	}

	// Persist result
	if result.Remaining != nil && result.Remaining.Quantity > 0 {
		err = ob.orderInsert(ctx, *result.Remaining)
		if err != nil {
			WithError(ctx)
			return nil, err
		}
	}
	for _, filledOrder := range result.Filled {
		err = ob.orderRemove(ctx, filledOrder)
		if err != nil {
			WithError(ctx)
			return nil, err
		}
	}
	if result.Partial != nil {
		err = ob.orderUpdate(ctx, *result.Partial)
		if err != nil {
			WithError(ctx)
			return nil, err
		}
	}

	return result.Trades, nil
}

// func (ob *Orderbook) Add(o *Order) {
// 	fn := func(o *Order) {}
// 	<-fn
// }

func (ob *Orderbook) Add(o *Order) ([]Trade, error) {
	ctx := WithUndo(context.Background())
	defer Rollback(ctx)
	if o == nil {
		return nil, ErrEmptyOrder
	}
	switch o.Type {
	case Bid:
		bidQueue, _ := ob.bids.Entries.Get(o.Price)
		_, exists := bidQueue.Entries.Get(o.ID)
		if exists {
			return nil, fmt.Errorf("get bid queue: %w", ErrOrderExists)
		}
		trades, err := ob.processLimitBuy(ctx, o)
		if err != nil {
			return nil, fmt.Errorf("process limit buy: %w", err)
		}
		ob.bids.Size += o.Quantity
		for _, trade := range trades {
			ob.bids.Size -= trade.Amount
			ob.asks.Size -= trade.Amount
		}

		// Cleanup empty maps
		bidQueue, bidQueueExists := ob.bids.Entries.Get(o.Price)
		if bidQueueExists && bidQueue.Entries.Len() == 0 {
			ob.bids.Entries = ob.bids.Entries.Remove(o.Price)
		}
		for _, trade := range trades {
			askQueue, askQueueExists := ob.asks.Entries.Get(trade.Price)
			if askQueueExists && askQueue.Entries.Len() == 0 {
				ob.asks.Entries = ob.asks.Entries.Remove(trade.Price)
			}
		}

		return trades, nil
	case Ask:
		askQueue, _ := ob.asks.Entries.Get(o.Price)
		_, exists := askQueue.Entries.Get(o.ID)
		if exists {
			return nil, fmt.Errorf("get ask queue: %w", ErrOrderExists)

		}
		trades, err := ob.processLimitSell(ctx, o)
		if err != nil {
			return nil, fmt.Errorf("process limit sell: %w", err)
		}
		ob.asks.Size += o.Quantity
		for _, trade := range trades {
			ob.asks.Size -= trade.Amount
			ob.bids.Size -= trade.Amount
		}
		// Cleanup empty maps
		askQueue, askQueueExists := ob.asks.Entries.Get(o.Price)
		if askQueueExists && askQueue.Entries.Len() == 0 {
			ob.asks.Entries = ob.asks.Entries.Remove(o.Price)
		}
		for _, trade := range trades {
			bidQueue, bidQueueExists := ob.bids.Entries.Get(trade.Price)
			if bidQueueExists && bidQueue.Entries.Len() == 0 {
				ob.bids.Entries = ob.bids.Entries.Remove(trade.Price)
			}
		}
		return trades, nil
	default:
		return nil, ErrInvalidSide
	}
}

func (ob *Orderbook) Bids() (uint64, []Order) {
	result := []Order{}
	bidQueue := ob.bids.Entries.Iterate()
	for i := bidQueue; !i.Done(); i.Next() {
		orderQueue := i.GetValue().Entries.Iterate()
		for j := orderQueue; !j.Done(); j.Next() {
			result = append(result, j.GetValue())
		}
	}
	return ob.bids.Size, result
}

func (ob *Orderbook) Asks() (uint64, []Order) {
	result := []Order{}
	askQueue := ob.asks.Entries
	for i := askQueue.Iterate(); !i.Done(); i.Next() {
		orderQueue := i.GetValue().Entries
		for j := orderQueue.Iterate(); !j.Done(); j.Next() {
			result = append(result, j.GetValue())
		}
	}
	return ob.asks.Size, result
}

func (ob *Orderbook) processLimitBuy(ctx context.Context, o *Order) ([]Trade, error) {
	lowestAsk, err := ob.lowestAsk()
	if err != nil && !errors.Is(err, ErrNoLiquidity) {
		WithError(ctx)
		return nil, fmt.Errorf("lowest ask: %w", err)
	}
	if err == ErrNoLiquidity || o.Price < lowestAsk.Price {

		// No matching orders required
		err = ob.orderInsert(ctx, *o)
		if err != nil && !errors.Is(err, ErrNoLiquidity) {
			WithError(ctx)
			return nil, fmt.Errorf("insert order: %w", err)
		}
		return []Trade{}, nil
	}
	// Matching orders required
	result, err := ob.calculateLimitBuy(o)
	if err != nil {
		WithError(ctx)
		return nil, fmt.Errorf("calculate result: %w", err)
	}

	// Persist result
	if result.Remaining != nil && result.Remaining.Quantity > 0 {
		err = ob.orderInsert(ctx, *result.Remaining)
		if err != nil {
			WithError(ctx)
			return nil, fmt.Errorf("insert order: %w", err)
		}
	}
	for _, filledOrder := range result.Filled {
		err = ob.orderRemove(ctx, filledOrder)
		if err != nil {
			WithError(ctx)
			return nil, fmt.Errorf("remove filled order: %w", err)
		}
	}
	if result.Partial != nil {
		err = ob.orderUpdate(ctx, *result.Partial)
		if err != nil {
			WithError(ctx)
			return nil, fmt.Errorf("update partially filled order: %w", err)
		}
	}

	return result.Trades, nil
}

type LimitResult struct {
	Remaining *Order
	Partial   *Order
	Filled    []Order
	Trades    []Trade
}

func (ob *Orderbook) calculateLimitBuy(o *Order) (*LimitResult, error) {
	remainingQuantity := o.Quantity

	filled := []Order{}
	var partial *Order
	// Matching orders required
	// Calculate full matches
	askQueue := ob.asks.Entries.Iterate()
	for askNode := askQueue; !askNode.Done(); askNode.Next() {
		// Get orders within a single bid/ask matching price
		askPrice := askNode.GetKey()
		if o.Price < askPrice {
			break
		}
		fmt.Println("filling price", askPrice)
		if remainingQuantity == 0 {
			break
		}
		orderQueue := askNode.GetValue().Entries.Iterate()
		for orderNode := orderQueue; !orderNode.Done(); orderNode.Next() {
			// Match orders
			if remainingQuantity == 0 {
				break
			}
			matchedOrder := orderNode.GetValue()
			matchedQuantity := matchedOrder.Quantity
			if matchedOrder.Quantity > remainingQuantity {
				matchedQuantity = matchedOrder.Quantity - remainingQuantity
			}
			fmt.Printf("\tmatching order %d, qty %d price %d\n", matchedOrder.ID, matchedQuantity, matchedOrder.Price)

			if remainingQuantity < matchedOrder.Quantity {
				partial = &Order{
					ID:        matchedOrder.ID,
					Type:      matchedOrder.Type,
					Quantity:  matchedQuantity,
					Price:     matchedOrder.Price,
					CreatedAt: matchedOrder.CreatedAt,
				}
				remainingQuantity = 0
				break
			}
			if remainingQuantity > matchedOrder.Quantity {
				filled = append(filled, matchedOrder)
				remainingQuantity -= matchedQuantity
				continue
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
		trades = append(trades, Trade{AskOrderID: filledOrder.ID, BidOrderID: o.ID, Amount: filledOrder.Quantity, Price: o.Price})
	}
	if partial != nil {
		trades = append(trades, Trade{ID: uuid.Must(uuid.NewV4()), AskOrderID: partial.ID, BidOrderID: o.ID, Amount: partial.Quantity, Price: o.Price})
	}
	return &LimitResult{
		Remaining: remainingOrder,
		Partial:   partial,
		Filled:    filled,
		Trades:    trades,
	}, nil
}

func (ob *Orderbook) calculateLimitSell(o *Order) (*LimitResult, error) {
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

			matchedQuantity := matchedOrder.Quantity
			if matchedOrder.Quantity > remainingQuantity {
				matchedQuantity = matchedOrder.Quantity - remainingQuantity
			}
			fmt.Printf("\tmatching order %d, qty %d price %d:\n", matchedOrder.ID, matchedQuantity, matchedOrder.Price)
			if remainingQuantity < matchedOrder.Quantity {
				partial = &Order{
					ID:        matchedOrder.ID,
					Type:      matchedOrder.Type,
					Quantity:  matchedQuantity,
					Price:     matchedOrder.Price,
					CreatedAt: matchedOrder.CreatedAt,
				}
				remainingQuantity = 0
				break
			}
			if remainingQuantity > matchedOrder.Quantity {
				filled = append(filled, matchedOrder)
				remainingQuantity -= matchedQuantity
				break
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

	return &LimitResult{
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

type undoFn func()

type Undo struct {
	fns      []undoFn
	hasError bool
}

type UndoContextKey string

const UndoKey UndoContextKey = "undo"

func WithUndo(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, UndoKey, &Undo{})
	return ctx
}
func WithError(ctx context.Context) {
	u := ctx.Value(UndoKey).(*Undo)
	u.hasError = true
}
func WithOp(ctx context.Context, fn func()) {
	u := ctx.Value(UndoKey).(*Undo)
	u.fns = append(u.fns, fn)
}
func Rollback(ctx context.Context) error {
	u := ctx.Value(UndoKey).(*Undo)
	if u.hasError {
		return ErrNoError
	}
	for i := len(u.fns) - 1; i >= 0; i-- {
		u.fns[i]()
	}
	return nil
}

// orderInsert a new order
func (ob *Orderbook) orderInsert(ctx context.Context, o Order) error {
	switch o.Type {
	case Bid:
		oq, exists := ob.bids.Entries.Get(o.Price)
		if !exists {
			ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		// oq.Size += o.Quantity
		WithOp(ctx, func() { oq.Entries = oq.Entries.Remove(o.ID) })
	case Ask:
		oq, exists := ob.asks.Entries.Get(o.Price)
		if !exists {
			ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		// oq.Size += o.Quantity
		WithOp(ctx, func() { oq.Entries = oq.Entries.Remove(o.ID) })
	}
	return nil
}

// orderUpdate a specific order because it was partially filled
func (ob *Orderbook) orderUpdate(ctx context.Context, o Order) error {
	switch o.Type {
	case Bid:
		oq, exists := ob.bids.Entries.Get(o.Price)
		if !exists {
			ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		}
		original, exists := oq.Entries.Get(o.ID)
		if !exists {
			WithError(ctx)
			return fmt.Errorf("get order %d: %w", o.ID, ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		WithOp(ctx, func() { oq.Entries = oq.Entries.Insert(o.ID, original) })
	case Ask:
		oq, exists := ob.asks.Entries.Get(o.Price)
		if !exists {
			ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		}
		original, exists := oq.Entries.Get(o.ID)
		if !exists {
			WithError(ctx)
			return fmt.Errorf("get order %d: %w", o.ID, ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		WithOp(ctx, func() { oq.Entries = oq.Entries.Insert(o.ID, original) })
	default:
		return ErrInvalidSide
	}
	return nil
}

func (ob *Orderbook) orderRemove(ctx context.Context, o Order) error {
	switch o.Type {
	case Bid:
		oq, exists := ob.bids.Entries.Get(o.Price)
		if !exists {
			WithError(ctx)
			return fmt.Errorf("get orderqueue: %w", ErrOrderQueueNotExist)
		}
		_, exists = oq.Entries.Get(o.ID)
		if !exists {
			WithError(ctx)
			return fmt.Errorf("get order %d: %w", o.ID, ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Remove(o.ID)
		ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		WithOp(ctx, func() { oq.Entries = oq.Entries.Insert(o.ID, o) })
		return nil
	case Ask:
		oq, exists := ob.asks.Entries.Get(o.Price)
		if !exists {
			WithError(ctx)
			return fmt.Errorf("get orderqueue: %w", ErrOrderQueueNotExist)
		}
		_, exists = oq.Entries.Get(o.ID)
		if !exists {
			WithError(ctx)
			return fmt.Errorf("get order %d: %w", o.ID, ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Remove(o.ID)
		ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		WithOp(ctx, func() { oq.Entries = oq.Entries.Insert(o.ID, o) })
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
	if ob.asks.Entries.Len() <= 0 {
		return Order{}, ErrNoLiquidity
	}
	return ob.asks.Entries.Min().V.Entries.Max().V, nil
}
func (ob *Orderbook) Cancel(o *Order) error {
	var oq OrderQueue
	exists := false
	if o.Type == Bid {
		oq, exists = ob.bids.Entries.Get(o.Price)
		if !exists {
			return fmt.Errorf("get order queue %s %d: %w", "bid", o.Price, ErrOrderQueueNotExist)
		}
	} else {
		oq, exists = ob.asks.Entries.Get(o.Price)
		if !exists {
			return fmt.Errorf("get order queue %s %d: %w", "ask", o.Price, ErrOrderQueueNotExist)
		}
	}

	_, exists = oq.Entries.Get(o.ID)
	if !exists {
		return fmt.Errorf("get order %d: %w", o.ID, ErrOrderNotExist)
	}

	oq.Entries = oq.Entries.Remove(o.ID)
	if o.Type == Bid {
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

func (ob *Orderbook) MarketSell(orderID uint64, quantity uint64) ([]Trade, error) {
	if ob.bids.Size < quantity {
		return []Trade{}, ErrNoLiquidity
	}
	return ob.Add(&Order{
		ID:        orderID,
		Type:      Ask,
		Quantity:  quantity,
		Price:     0,
		CreatedAt: time.Now(),
	})
}
func (ob *Orderbook) MarketBuy(orderID uint64, quantity uint64) ([]Trade, error) {
	if ob.asks.Size < quantity {
		return []Trade{}, ErrNoLiquidity
	}
	return ob.Add(&Order{
		ID:        orderID,
		Type:      Bid,
		Quantity:  quantity,
		Price:     math.MaxUint64,
		CreatedAt: time.Now(),
	})
}
