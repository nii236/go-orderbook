package orderbook_test

import (
	"fmt"
	"math/rand"
	ob "orderbook"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"testing"
	"time"
)

func BenchmarkOrderbook(b *testing.B) {
	var orderID atomic.Uint64
	book := ob.NewOrderbook("TEST")
	for n := 0; n < b.N; n++ {
		qty := uint64(rand.Intn(20)) + 1
		price := 90 + uint64(rand.Intn(20))
		book.Add(&ob.Order{
			ID:        orderID.Add(1),
			Type:      ob.Ask,
			Quantity:  qty,
			Price:     price,
			CreatedAt: time.Now(),
		})
		qty = uint64(rand.Intn(20)) + 1
		price = 93 + uint64(rand.Intn(20))
		book.Add(&ob.Order{
			ID:        orderID.Add(1),
			Type:      ob.Bid,
			Quantity:  qty,
			Price:     price,
			CreatedAt: time.Now(),
		})
	}
}

func TestOrderbook_AddAsks(t *testing.T) {
	book := ob.NewOrderbook("TEST")

	_, err := book.Add(&ob.Order{
		ID:        2,
		Type:      ob.Ask,
		Quantity:  1,
		Price:     102,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	trades, err := book.Add(&ob.Order{
		ID:        1,
		Type:      ob.Ask,
		Quantity:  5,
		Price:     102,
		CreatedAt: time.Now(),
	})

	assert.Nil(t, err)
	askSize, asks := book.Asks()
	assert.EqualValues(t, askSize, 6, "need more asks")
	assert.Equal(t, len(asks), 2, "need more asks")
	assert.Equal(t, len(trades), 0, "no trades expected")
}
func TestOrderbook_AddBids(t *testing.T) {
	book := ob.NewOrderbook("TEST")
	_, err := book.Add(&ob.Order{
		ID:        1,
		Type:      ob.Ask,
		Quantity:  5,
		Price:     102,
		CreatedAt: time.Now(),
	})

	assert.Nil(t, err)
	trades, err := book.Add(&ob.Order{
		ID:        2,
		Type:      ob.Ask,
		Quantity:  1,
		Price:     102,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	askSize, asks := book.Asks()
	assert.EqualValues(t, askSize, 6, "need more asks")
	assert.Equal(t, len(asks), 2, "need more asks")
	assert.Equal(t, len(trades), 0, "no trades expected")
}
func TestOrderbook_MarketBuy(t *testing.T) {
	book := ob.NewOrderbook("TEST")
	_, err := book.Add(&ob.Order{
		ID:        1,
		Type:      ob.Ask,
		Quantity:  2,
		Price:     99,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	_, err = book.Add(&ob.Order{
		ID:        2,
		Type:      ob.Ask,
		Quantity:  2,
		Price:     100,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	_, err = book.MarketBuy(3, 5)
	assert.NotNil(t, err)
	trades, err := book.MarketBuy(3, 4)
	assert.Nil(t, err)
	bidSize, _ := book.Bids()
	askSize, _ := book.Asks()
	assert.Equal(t, 2, len(trades), "expected 2 trade")
	assert.EqualValues(t, 0, bidSize, "bidqueue not empty")
	assert.EqualValues(t, 0, askSize, "askqueue not empty")
}
func TestOrderbook_MarketSell(t *testing.T) {
	book := ob.NewOrderbook("TEST")
	_, err := book.Add(&ob.Order{
		ID:        1,
		Type:      ob.Bid,
		Quantity:  2,
		Price:     99,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	_, err = book.Add(&ob.Order{
		ID:        2,
		Type:      ob.Bid,
		Quantity:  2,
		Price:     100,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	_, err = book.MarketSell(3, 5)
	assert.NotNil(t, err)
	trades, err := book.MarketSell(3, 4)
	assert.Nil(t, err)
	bidSize, _ := book.Bids()
	askSize, _ := book.Asks()
	assert.Equal(t, 2, len(trades), "expected 2 trade")
	assert.EqualValues(t, 0, bidSize, "bidqueue not empty")
	assert.EqualValues(t, 0, askSize, "askqueue not empty")
}
func TestOrderbook_PartialFilledBid(t *testing.T) {
	book := ob.NewOrderbook("TEST")
	_, err := book.Add(&ob.Order{
		ID:        1,
		Type:      ob.Bid,
		Quantity:  5,
		Price:     99,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	_, err = book.Add(&ob.Order{
		ID:        2,
		Type:      ob.Bid,
		Quantity:  2,
		Price:     100,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	trades, err := book.Add(&ob.Order{
		ID:        3,
		Type:      ob.Ask,
		Quantity:  3,
		Price:     98,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	bidSize, bids := book.Bids()
	askSize, _ := book.Asks()
	fmt.Println(book)
	assert.Equal(t, 1, len(bids), "need more bids")
	assert.EqualValues(t, 4, bidSize, "need more bid size")
	assert.EqualValues(t, 0, askSize, "need zero asks")
	assert.Equal(t, 2, len(trades), "expected 2 trade")
	assert.EqualValues(t, 3, trades[0].Quantity+trades[1].Quantity, "trades should total qty 3")
	assert.EqualValues(t, 1, trades[1].Quantity, "1 trade amount expected for second trade")
}
func TestOrderbook_FilledBid(t *testing.T) {
	book := ob.NewOrderbook("TEST")
	_, err := book.Add(&ob.Order{
		ID:        1,
		Type:      ob.Bid,
		Quantity:  1,
		Price:     100,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	trades, err := book.Add(&ob.Order{
		ID:        2,
		Type:      ob.Ask,
		Quantity:  1,
		Price:     99,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(trades), "expected 1 trade")
}
func TestOrderbook_PartialFilledAsk(t *testing.T) {
	book := ob.NewOrderbook("TEST")
	_, err := book.Add(&ob.Order{
		ID:        2,
		Type:      ob.Ask,
		Quantity:  3,
		Price:     100,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	_, err = book.Add(&ob.Order{
		ID:        1,
		Type:      ob.Ask,
		Quantity:  5,
		Price:     99,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	trades, err := book.Add(&ob.Order{
		ID:        3,
		Type:      ob.Bid,
		Quantity:  6,
		Price:     105,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	bidSize, _ := book.Bids()
	askSize, asks := book.Asks()
	assert.Equal(t, 1, len(asks), "need more asks")
	assert.EqualValues(t, 2, askSize, "need more ask size")
	assert.EqualValues(t, 0, bidSize, "need zero bids")
	assert.Equal(t, 2, len(trades), "expected 2 trade")
	assert.EqualValues(t, 6, trades[0].Quantity+trades[1].Quantity, "trades should total qty 6")
	assert.EqualValues(t, 1, trades[1].Quantity, "different trade amount expected for second trade")
}
func TestOrderbook_FilledAsk(t *testing.T) {
	book := ob.NewOrderbook("TEST")
	_, err := book.Add(&ob.Order{
		ID:        1,
		Type:      ob.Ask,
		Quantity:  1,
		Price:     100,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	trades, err := book.Add(&ob.Order{
		ID:        2,
		Type:      ob.Bid,
		Quantity:  1,
		Price:     101,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(trades), "expected 1 trade")
}
func TestOrderbook_Cancel(t *testing.T) {
	type fields struct {
		orders []*ob.Order
	}
	type args struct {
		o *ob.Order
	}
	bidOrder := &ob.Order{ID: 1, Type: ob.Bid, Quantity: 1, Price: 100, CreatedAt: time.Now()}
	askOrder := &ob.Order{ID: 1, Type: ob.Ask, Quantity: 1, Price: 100, CreatedAt: time.Now()}
	cancelBidOrder := &ob.Order{1, ob.Bid, 1, 100, time.Now()}
	cancelAskOrder := &ob.Order{1, ob.Ask, 1, 100, time.Now()}
	cancelMissingOrder := &ob.Order{2, ob.Bid, 100, 10, time.Now()}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		wantLenBids int
		wantLenAsks int
	}{
		{"empty book", fields{orders: []*ob.Order{}}, args{cancelBidOrder}, true, 0, 0},
		{"bid order exists", fields{orders: []*ob.Order{bidOrder}}, args{cancelBidOrder}, false, 0, 0},
		{"ask order exists", fields{orders: []*ob.Order{askOrder}}, args{cancelAskOrder}, false, 0, 0},
		{"ask order exists, wrong side", fields{orders: []*ob.Order{askOrder}}, args{cancelBidOrder}, true, 1, 0},
		{"order missing", fields{orders: []*ob.Order{}}, args{cancelMissingOrder}, true, 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			book := ob.NewOrderbook("TEST")
			for _, o := range tt.fields.orders {
				_, err := book.Add(o)
				assert.Nil(t, err)
			}
			if err := book.Cancel(tt.args.o); (err != nil) != tt.wantErr {
				t.Errorf("book.Cancel() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				_, bids := book.Bids()
				_, asks := book.Asks()
				if len(bids) != tt.wantLenBids {
					t.Errorf("wrong bid length, got %v want %v", len(bids), tt.wantLenBids)
				}
				if len(asks) != tt.wantLenAsks {
					t.Errorf("wrong ask length, got %v want %v", len(asks), tt.wantLenAsks)
				}
			}
		})
	}
}
