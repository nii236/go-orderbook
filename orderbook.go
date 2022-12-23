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

// OrderType for a bid or ask
type OrderType string

// Bid, or buy
const Bid OrderType = "BID"

// Ask, or sell
const Ask OrderType = "ASK"

// Order to enter or remove from the orderbook
// This is the struct most users will interact with
type Order struct {
	ID        uint64
	Type      OrderType
	Quantity  uint64
	Price     uint64
	CreatedAt time.Time
}

// OrderQueue is an ordered map for a specific price point
type OrderQueue struct {
	Size    uint64
	Entries ordmap.NodeBuiltin[uint64, Order]
}

// PriceMap is an ordered map for another ordered map (OrderQueue)
type PriceMap struct {
	Size    uint64
	Entries ordmap.NodeBuiltin[uint64, OrderQueue]
}

// NewPriceMap
func NewPriceMap(entries []*Order) *PriceMap {
	pm := &PriceMap{Entries: ordmap.NewBuiltin[uint64, OrderQueue]()}
	for _, entry := range entries {
		oq, _ := pm.Entries.Get(entry.Price)
		oq.Entries = oq.Entries.Insert(entry.ID, *entry)
		pm.Entries = pm.Entries.Insert(entry.Price, oq)
	}
	return pm
}

// Orderbook holding two ordered map of ordered maps (bids and asks)
type Orderbook struct {
	Symbol string
	bids   *PriceMap
	asks   *PriceMap
	addCh  chan func()
}

// NewOrderbook that starts off empty
func NewOrderbook(symbol string) *Orderbook {
	ob := &Orderbook{
		Symbol: symbol,
		bids:   NewPriceMap([]*Order{}),
		asks:   NewPriceMap([]*Order{}),
		addCh:  make(chan func()),
	}
	go ob.run()
	return ob
}

// run is automatically called on NewOrderbook to start listening for order add and cancel requests
func (ob *Orderbook) run() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			fmt.Println(ob)
		}
	}()
	for fn := range ob.addCh {
		fn()
	}
}

// String provides friendly formatting for fmt.Println
func (ob *Orderbook) String() string {
	result := ""
	askQueue := ob.asks.Entries.IterateReverse()
	for askNode := askQueue; !askNode.Done(); askNode.Next() {
		result += fmt.Sprintf("%s %d USD: ", "ASK", askNode.GetKey())
		orderQueue := askNode.GetValue().Entries.Iterate()
		for orderNode := orderQueue; !orderNode.Done(); orderNode.Next() {
			result += fmt.Sprintf("%d %s", orderNode.GetValue().Quantity, ob.Symbol)
		}
		result += "\n"
	}
	result += "\n"
	bidQueue := ob.bids.Entries.IterateReverse()
	for bidNode := bidQueue; !bidNode.Done(); bidNode.Next() {
		result += fmt.Sprintf("%s %d USD: ", "BID", bidNode.GetKey())
		orderQueue := bidNode.GetValue().Entries.Iterate()
		for orderNode := orderQueue; !orderNode.Done(); orderNode.Next() {
			result += fmt.Sprintf("%d %s", orderNode.GetValue().Quantity, ob.Symbol)
		}
		result += "\n"
	}
	return result
}

func (ob *Orderbook) processLimitSell(ctx context.Context, o *Order) ([]Trade, error) {
	highestBid, err := ob.HighestBid()
	if err != nil && !errors.Is(err, ErrNoLiquidity) {
		WithError(ctx, fmt.Errorf("can not get highest bid: %w", err))
		return nil, fmt.Errorf("process limit sell: %w", err)
	}

	// No matching between orders required
	// Just insert into orerbook
	if err == ErrNoLiquidity || o.Price > highestBid {
		ob.orderInsert(ctx, *o)
		return []Trade{}, nil
	}

	// Matching orders required
	// Calculate changes
	result := ob.calculateLimitSell(o)

	// Persist result
	if result.Remaining != nil && result.Remaining.Quantity > 0 {
		ob.orderInsert(ctx, *result.Remaining)
	}
	for _, filledOrder := range result.Filled {
		err = ob.orderRemove(ctx, filledOrder)
		if err != nil {
			WithError(ctx, fmt.Errorf("can not remove error: %w", err))
			return nil, err
		}
	}
	if result.Partial != nil {
		err = ob.orderUpdate(ctx, *result.Partial)
		if err != nil {
			WithError(ctx, fmt.Errorf("can not update error: %w", err))
			return nil, err
		}
	}

	return result.Trades, nil
}

// Add an order into the orderbook
// User facing function
// Threadsafe
func (ob *Orderbook) Add(o *Order) ([]Trade, error) {
	if o.Quantity <= 0 {
		return []Trade{}, ErrEmptyOrder
	}

	tradesCh := make(chan []Trade)
	errCh := make(chan error)
	fn := func() {
		trades, err := add(ob, o)
		tradesCh <- trades
		errCh <- err
	}
	ob.addCh <- fn
	trades := <-tradesCh
	err := <-errCh
	return trades, err
}

// add an order into the orderbook
// Internal function
// Not threadsafe
// Cleans up empty PriceMaps
// Rollback operations on error
func add(ob *Orderbook, o *Order) ([]Trade, error) {
	ctx := WithUndo(context.Background())
	defer func() {
		err := Rollback(ctx)
		if err == nil {
			fmt.Println("ROLLBACK EXECUTED")
			fmt.Println(Errors(ctx))
		}
	}()
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
			ob.bids.Size -= trade.Quantity
			ob.asks.Size -= trade.Quantity
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
			ob.asks.Size -= trade.Quantity
			ob.bids.Size -= trade.Quantity
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

// Map of the bids and asks
func (ob *Orderbook) Map() (*PriceMap, *PriceMap) {
	return ob.bids, ob.asks
}

// Bids full volume, and slice of Orders
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

// Asks full volume, and slice of Orders
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

// processLimitBuy runs through the logic of calculating trades, new orders
// and partially filled orders then persisting them
func (ob *Orderbook) processLimitBuy(ctx context.Context, o *Order) ([]Trade, error) {
	lowestAsk, err := ob.LowestAsk()
	if err != nil && !errors.Is(err, ErrNoLiquidity) {
		WithError(ctx, fmt.Errorf("can not get lowest ask: %w", err))
		return nil, fmt.Errorf("lowest ask: %w", err)
	}

	// No matching between orders required
	// Just insert into orerbook
	if err == ErrNoLiquidity || o.Price < lowestAsk {
		ob.orderInsert(ctx, *o)
		return []Trade{}, nil
	}

	// Matching orders required
	// Calculate changes
	result := ob.calculateLimitBuy(o)

	// Persist result
	if result.Remaining != nil && result.Remaining.Quantity > 0 {
		ob.orderInsert(ctx, *result.Remaining)
	}
	for _, filledOrder := range result.Filled {
		err = ob.orderRemove(ctx, filledOrder)
		if err != nil {
			WithError(ctx, fmt.Errorf("can not remove order: %w", err))
			return nil, fmt.Errorf("remove filled order: %w", err)
		}
	}
	if result.Partial != nil {
		err = ob.orderUpdate(ctx, *result.Partial)
		if err != nil {
			WithError(ctx, fmt.Errorf("can not update order: %w", err))
			return nil, fmt.Errorf("update partially filled order: %w", err)
		}
	}

	return result.Trades, nil
}

// LimitResult are the affected orders after a limit order is processed
type LimitResult struct {
	Remaining *Order
	Partial   *Order
	Filled    []Order
	Trades    []Trade
}

// calculateLimitBuy works out the order changes without persisting anything
func (ob *Orderbook) calculateLimitBuy(o *Order) *LimitResult {
	remainingQuantity := o.Quantity

	filled := []Order{}
	var partial *Order
	partialTradeQuantity := uint64(0)
	// Matching orders required
	// Calculate full matches
	askQueue := ob.asks.Entries.Iterate()
	for askNode := askQueue; !askNode.Done(); askNode.Next() {
		// Get orders within a single bid/ask matching price
		askPrice := askNode.GetKey()
		if o.Price < askPrice {
			break
		}
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

			if remainingQuantity < matchedOrder.Quantity {
				partial = &Order{
					ID:        matchedOrder.ID,
					Type:      matchedOrder.Type,
					Quantity:  matchedQuantity,
					Price:     matchedOrder.Price,
					CreatedAt: matchedOrder.CreatedAt,
				}
				partialTradeQuantity = remainingQuantity
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
		trades = append(trades, Trade{AskOrderID: filledOrder.ID, BidOrderID: o.ID, Quantity: filledOrder.Quantity, Price: filledOrder.Price})
	}
	if partial != nil {
		trades = append(trades, Trade{ID: uuid.Must(uuid.NewV4()), AskOrderID: partial.ID, BidOrderID: o.ID, Quantity: partialTradeQuantity, Price: partial.Price})
	}
	return &LimitResult{
		Remaining: remainingOrder,
		Partial:   partial,
		Filled:    filled,
		Trades:    trades,
	}
}

// calculateLimitSell works out the order changes without persisting anything
func (ob *Orderbook) calculateLimitSell(o *Order) *LimitResult {
	remainingQuantity := o.Quantity
	filled := []Order{}
	var partial *Order
	partialTradeQuantity := uint64(0)
	// Matching orders required
	// Calculate full matches
	bidQueue := ob.bids.Entries.IterateReverse()
	for bidNode := bidQueue; !bidNode.Done(); bidNode.Next() {
		// Get orders within a single bid/ask matching price
		bidPrice := bidNode.GetKey()
		if o.Price > bidPrice {
			break
		}
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
			if remainingQuantity < matchedOrder.Quantity {
				partial = &Order{
					ID:        matchedOrder.ID,
					Type:      matchedOrder.Type,
					Quantity:  matchedQuantity,
					Price:     matchedOrder.Price,
					CreatedAt: matchedOrder.CreatedAt,
				}
				partialTradeQuantity = remainingQuantity
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
		trades = append(trades, Trade{AskOrderID: o.ID, BidOrderID: filledOrder.ID, Quantity: filledOrder.Quantity, Price: filledOrder.Price})
	}
	if partial != nil {
		trades = append(trades, Trade{ID: uuid.Must(uuid.NewV4()), AskOrderID: o.ID, BidOrderID: partial.ID, Quantity: partialTradeQuantity, Price: partial.Price})
	}

	return &LimitResult{
		Remaining: remainingOrder,
		Partial:   partial,
		Filled:    filled,
		Trades:    trades,
	}
}

// Trade is the filling between two orders
type Trade struct {
	ID         uuid.UUID
	BidOrderID uint64
	AskOrderID uint64
	Price      uint64
	Quantity   uint64
}

// Undo holds functions to rollback a single processed order
type Undo struct {
	fns      []func()
	hasError bool
	errors   []error
}

// UndoContextKey to prevent context key collisions
type UndoContextKey string

// UndoKey used to set and get values from context
const UndoKey UndoContextKey = "undo"

// WithUndo returns a new context holding a pointer to the Undo struct
func WithUndo(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, UndoKey, &Undo{})
	return ctx
}

// WithError marks this context as requiring an Undo
func WithError(ctx context.Context, err error) {
	u := ctx.Value(UndoKey).(*Undo)
	u.errors = append(u.errors, err)
	u.hasError = true
}

// WithUndoOp appends an anonymous function to the Undo struct, for rollback execution
func WithUndoOp(ctx context.Context, fn func()) {
	u := ctx.Value(UndoKey).(*Undo)
	u.fns = append(u.fns, fn)
}

// Errors persisted during order execution
func Errors(ctx context.Context) []error {
	u := ctx.Value(UndoKey).(*Undo)
	if u.hasError {
		return []error{}
	}
	return u.errors
}

// Rollback by running the persisted operations in reverse
func Rollback(ctx context.Context) error {
	u := ctx.Value(UndoKey).(*Undo)
	if !u.hasError {
		return ErrNoError
	}
	for i := len(u.fns) - 1; i >= 0; i-- {
		u.fns[i]()
	}
	return nil
}

// orderInsert a new order
func (ob *Orderbook) orderInsert(ctx context.Context, o Order) {
	switch o.Type {
	case Bid:
		oq, exists := ob.bids.Entries.Get(o.Price)
		if !exists {
			ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		oq.Size += o.Quantity
		ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		WithUndoOp(ctx, func() {
			oq.Size -= o.Quantity
			oq.Entries = oq.Entries.Remove(o.ID)
			ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		})
	case Ask:
		oq, exists := ob.asks.Entries.Get(o.Price)
		if !exists {
			ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		oq.Size += o.Quantity
		ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		WithUndoOp(ctx, func() {
			oq.Size -= o.Quantity
			oq.Entries = oq.Entries.Remove(o.ID)
			ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		})
	}
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
			WithError(ctx, fmt.Errorf("can not update order, not found: %d", o.ID))
			return fmt.Errorf("get order %d: %w", o.ID, ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		oq.Size -= original.Quantity - o.Quantity
		ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		WithUndoOp(ctx, func() {
			oq.Size += original.Quantity - o.Quantity
			oq.Entries = oq.Entries.Insert(o.ID, original)
			ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		})
	case Ask:
		oq, exists := ob.asks.Entries.Get(o.Price)
		if !exists {
			ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		}
		original, exists := oq.Entries.Get(o.ID)
		if !exists {
			WithError(ctx, fmt.Errorf("can not update order, not found: %d", o.ID))
			return fmt.Errorf("get order %d: %w", o.ID, ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Insert(o.ID, o)
		oq.Size -= original.Quantity - o.Quantity
		ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		WithUndoOp(ctx, func() {
			oq.Size += original.Quantity - o.Quantity
			oq.Entries = oq.Entries.Insert(o.ID, original)
			ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		})
	default:
		return ErrInvalidSide
	}
	return nil
}

// orderRemove a single order
func (ob *Orderbook) orderRemove(ctx context.Context, o Order) error {
	switch o.Type {
	case Bid:
		oq, exists := ob.bids.Entries.Get(o.Price)
		if !exists {
			WithError(ctx, fmt.Errorf("can not get orderqueue, price not found: %d", o.Price))
			return fmt.Errorf("get orderqueue: %w", ErrOrderQueueNotExist)
		}
		_, exists = oq.Entries.Get(o.ID)
		if !exists {
			WithError(ctx, fmt.Errorf("can not get order, order not found: %d", o.Price))
			return fmt.Errorf("get order %d: %w", o.ID, ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Remove(o.ID)
		oq.Size -= o.Quantity
		ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		WithUndoOp(ctx, func() {
			oq.Size += o.Quantity
			oq.Entries = oq.Entries.Insert(o.ID, o)
			ob.bids.Entries = ob.bids.Entries.Insert(o.Price, oq)
		})
		return nil
	case Ask:
		oq, exists := ob.asks.Entries.Get(o.Price)
		if !exists {
			WithError(ctx, fmt.Errorf("can not get orderqueue, price not found: %d", o.Price))
			return fmt.Errorf("get orderqueue: %w", ErrOrderQueueNotExist)
		}
		_, exists = oq.Entries.Get(o.ID)
		if !exists {
			WithError(ctx, fmt.Errorf("can not get order, order not found: %d", o.Price))
			return fmt.Errorf("get order %d: %w", o.ID, ErrOrderNotExist)
		}
		oq.Entries = oq.Entries.Remove(o.ID)
		oq.Size -= o.Quantity
		ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		WithUndoOp(ctx, func() {
			oq.Size += o.Quantity
			oq.Entries = oq.Entries.Insert(o.ID, o)
			ob.asks.Entries = ob.asks.Entries.Insert(o.Price, oq)
		})
		return nil
	default:
		return ErrInvalidSide
	}
}

// HighestBid returns the price of the highest bid, and error if there aren't any bids
func (ob *Orderbook) HighestBid() (uint64, error) {
	if ob.bids.Entries.Len() <= 0 {
		return 0, ErrNoLiquidity
	}
	return ob.bids.Entries.Max().V.Entries.Min().V.Price, nil
}

// LowestAsk returns the price of the lowest ask, and error if there aren't any asks
func (ob *Orderbook) LowestAsk() (uint64, error) {
	if ob.asks.Entries.Len() <= 0 {
		return 0, ErrNoLiquidity
	}
	return ob.asks.Entries.Min().V.Entries.Min().V.Price, nil
}

// Cancel an order, threadsafe
func (ob *Orderbook) Cancel(o *Order) error {
	errCh := make(chan error)
	fn := func() {
		err := cancel(ob, o)
		errCh <- err
	}
	ob.addCh <- fn
	err := <-errCh
	return err
}

// cancel an order by removing it from the orderbook, not threadsafe
// If the orderqueue is empty, remove it
func cancel(ob *Orderbook, o *Order) error {
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

// MarketSell into the orderbook. Effectively a limit sell with 0 price
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

// MarketBuy into the orderbook. Effectively a limit buy with max price
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
