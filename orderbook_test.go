package orderbook_test

import (
	"fmt"
	ob "orderbook"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOrderbook_Asks(t *testing.T) {
	bidPriceMap := ob.NewPriceMap([]*ob.Order{})
	askPriceMap := ob.NewPriceMap([]*ob.Order{})
	book := ob.NewOrderbook("TEST", bidPriceMap, askPriceMap)
	go book.Run()
	err := book.Add(&ob.Order{
		ID:        1,
		Type:      ob.LimitSell,
		Quantity:  5,
		Price:     102,
		CreatedAt: time.Now(),
	})

	assert.Nil(t, err)
	err = book.Add(&ob.Order{
		ID:        2,
		Type:      ob.LimitSell,
		Quantity:  1,
		Price:     105,
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)
	// err = book.Add(&ob.Order{
	// 	ID:        3,
	// 	Type:      ob.LimitSell,
	// 	Quantity:  10,
	// 	Price:     105,
	// 	CreatedAt: time.Now(),
	// })
	// assert.Nil(t, err)
	fmt.Println(book)
}
func TestOrderbook_LimitSell(t *testing.T) {
	emptyPriceMap := ob.NewPriceMap([]*ob.Order{})
	book := ob.NewOrderbook("TEST", emptyPriceMap, emptyPriceMap)
	go book.Run()
	err := book.Add(&ob.Order{
		ID:        1,
		Type:      ob.LimitSell,
		Quantity:  1,
		Price:     100,
		CreatedAt: time.Now(),
	})
	fmt.Println("err test ", err)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(book.Asks()), "expected order to be added to book")

}
func TestOrderbook_Cancel(t *testing.T) {
	type fields struct {
		bids *ob.PriceMap
		asks *ob.PriceMap
	}
	type args struct {
		o *ob.CancelOrder
	}
	emptyPriceMap := ob.NewPriceMap([]*ob.Order{})
	existingPriceMap := ob.NewPriceMap([]*ob.Order{
		{
			ID:        1,
			Type:      ob.LimitBuy,
			Quantity:  1,
			Price:     100,
			CreatedAt: time.Now(),
		},
	})
	cancelOrder := &ob.CancelOrder{1, ob.LimitBuy, 100}
	cancelMissingOrder := &ob.CancelOrder{2, ob.LimitBuy, 100}
	cancelOrderWrongSide := &ob.CancelOrder{1, ob.LimitSell, 100}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		wantLenBids int
		wantLenAsks int
	}{
		{"nil order", fields{bids: emptyPriceMap, asks: emptyPriceMap}, args{}, true, 0, 0},
		{"empty book", fields{bids: emptyPriceMap, asks: emptyPriceMap}, args{cancelOrder}, true, 0, 0},
		{"order exists", fields{bids: existingPriceMap, asks: emptyPriceMap}, args{cancelOrder}, false, 0, 0},
		{"order exists, wrong side", fields{bids: existingPriceMap, asks: emptyPriceMap}, args{cancelOrderWrongSide}, true, 1, 0},
		{"order missing", fields{bids: existingPriceMap, asks: emptyPriceMap}, args{cancelMissingOrder}, true, 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ob := ob.NewOrderbook("TEST", tt.fields.bids, tt.fields.asks)

			go ob.Run()
			if err := ob.Cancel(tt.args.o); (err != nil) != tt.wantErr {
				t.Errorf("ob.Cancel() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				bids := ob.Bids()
				asks := ob.Asks()
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
