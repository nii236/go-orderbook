package main

import (
	"fmt"
	"log"
	"math/rand"
	ob "orderbook"
	"strconv"
	"sync"

	"go.uber.org/atomic"
	xrand "golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"

	"time"

	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
)

func main() {
	var orderID atomic.Uint64
	var tradeCount atomic.Uint64
	book := ob.NewOrderbook("BTC")
	_, err := book.Add(&ob.Order{
		ID:        orderID.Add(1),
		Type:      ob.Bid,
		Quantity:  2,
		Price:     99,
		CreatedAt: time.Now(),
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = book.Add(&ob.Order{
		ID:        orderID.Add(1),
		Type:      ob.Bid,
		Quantity:  2,
		Price:     100,
		CreatedAt: time.Now(),
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = book.Add(&ob.Order{
		ID:        orderID.Add(1),
		Type:      ob.Ask,
		Quantity:  3,
		Price:     102,
		CreatedAt: time.Now(),
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	bids, asks := book.Map()
	source := xrand.NewSource(uint64(time.Now().UnixNano()))
	poissonBid := distuv.Poisson{
		Lambda: 100.0,
		Src:    source,
	}
	poissonAsk := distuv.Poisson{
		Lambda: 100.0,
		Src:    source,
	}

	if err := ui.Init(); err != nil {
		log.Fatalf("failed to initialize termui: %v", err)
	}
	defer ui.Close()

	bc := widgets.NewBarChart()
	bc.Data = Data(bids, asks)
	bc.Labels = Labels(bids, asks)
	bc.Title = "Orderbook"
	bc.SetRect(0, 0, 170, 25)
	bc.BarWidth = 3
	bc.BarColors = BarColors(bids, asks)
	bc.LabelStyles = []ui.Style{ui.NewStyle(ui.ColorBlue)}
	bc.NumStyles = []ui.Style{ui.NewStyle(ui.ColorYellow)}

	l := widgets.NewList()
	l.Title = "Orders [0]"
	l.Rows = []string{}
	l.TextStyle = ui.NewStyle(ui.ColorYellow)
	l.WrapText = false
	l.SetRect(0, 25, 170, 50)

	l2 := widgets.NewList()
	l2.Title = "Trades [0]"
	l2.Rows = []string{}
	l2.TextStyle = ui.NewStyle(ui.ColorYellow)
	l2.WrapText = false
	l2.SetRect(50, 25, 170, 50)

	// bidT := time.NewTicker(1 * time.Millisecond)
	// askT := time.NewTicker(1 * time.Millisecond)
	renderT := time.NewTicker(1 * time.Millisecond)
	ui.Render(bc)
	ui.Render(l)
	ui.Render(l2)
	mutex := &sync.Mutex{}
	go func() {
		for range renderT.C {
			mutex.Lock()

			if len(l2.Rows) > 0 {
				l2.ScrollBottom()
			}
			if len(l.Rows) > 0 {
				l.ScrollBottom()
			}
			ui.Render(bc)
			ui.Render(l)
			ui.Render(l2)
			mutex.Unlock()
		}
	}()
	go func() {
		for {
			mutex.Lock()
			// time.Sleep(1 * time.Second)
			qty := uint64(rand.Intn(20)) + 1
			price := uint64(poissonBid.Rand())
			if price < 85 {
				mutex.Unlock()
				continue
			}
			if price > 125 {
				mutex.Unlock()
				continue
			}
			book.HighestBid()
			trades, err := book.Add(&ob.Order{
				ID:        orderID.Add(1),
				Type:      ob.Ask,
				Quantity:  qty,
				Price:     price,
				CreatedAt: time.Now(),
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			l.Rows = append(l.Rows, fmt.Sprintf("[ASK] %d @ %d USD", qty, price))
			for _, trade := range trades {
				l2.Rows = append(l2.Rows, fmt.Sprintf("[TRADE] %d @ %d USD", trade.Quantity, trade.Price))
			}
			tradeCount.Add(uint64(len(trades)))
			l2.Title = fmt.Sprintf("Trades [%d]", tradeCount.Load())
			l.Title = fmt.Sprintf("Orders [%d]", orderID.Load())
			bids, asks := book.Map()
			bc.BarColors = BarColors(bids, asks)
			bc.Data = Data(bids, asks)
			bc.Labels = Labels(bids, asks)
			mutex.Unlock()
		}
	}()
	go func() {
		for {
			mutex.Lock()
			// time.Sleep(1 * time.Second)
			qty := uint64(rand.Intn(20)) + 1
			price := uint64(poissonAsk.Rand())
			if price < 85 {
				mutex.Unlock()
				continue
			}
			if price > 125 {
				mutex.Unlock()
				continue
			}
			trades, err := book.Add(&ob.Order{
				ID:        orderID.Add(1),
				Type:      ob.Bid,
				Quantity:  qty,
				Price:     price,
				CreatedAt: time.Now(),
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			l.Rows = append(l.Rows, fmt.Sprintf("[BID] %d @ %d USD", qty, price))
			for _, trade := range trades {
				l2.Rows = append(l2.Rows, fmt.Sprintf("[TRADE] %d @ %d USD", trade.Quantity, trade.Price))
			}
			tradeCount.Add(uint64(len(trades)))
			l2.Title = fmt.Sprintf("Trades [%d]", tradeCount.Load())
			l.Title = fmt.Sprintf("Orders [%d]", orderID.Load())
			bids, asks := book.Map()
			bc.BarColors = BarColors(bids, asks)
			bc.Data = Data(bids, asks)
			bc.Labels = Labels(bids, asks)
			mutex.Unlock()
		}
	}()

	uiEvents := ui.PollEvents()
	for {
		e := <-uiEvents
		switch e.ID {
		case "q", "<C-c>":
			return
		}
	}
}

func BarColors(bids *ob.PriceMap, asks *ob.PriceMap) []ui.Color {
	result := []ui.Color{}
	bidQueue := bids.Entries.Iterate()
	for bidNode := bidQueue; !bidNode.Done(); bidNode.Next() {
		result = append(result, ui.ColorRed)

	}
	askQueue := asks.Entries.Iterate()
	for askNode := askQueue; !askNode.Done(); askNode.Next() {
		result = append(result, ui.ColorGreen)
	}

	return result
}
func Labels(bids *ob.PriceMap, asks *ob.PriceMap) []string {
	labels := []string{}
	bidQueue := bids.Entries.Iterate()
	for bidNode := bidQueue; !bidNode.Done(); bidNode.Next() {
		entry := bidNode.GetKey()
		labels = append(labels, strconv.Itoa(int(entry)))
	}
	askQueue := asks.Entries.Iterate()
	for askNode := askQueue; !askNode.Done(); askNode.Next() {
		entry := askNode.GetKey()
		labels = append(labels, strconv.Itoa(int(entry)))
	}
	return labels
}
func Data(bids *ob.PriceMap, asks *ob.PriceMap) []float64 {
	data := []float64{}

	bidQueue := bids.Entries.Iterate()
	for bidNode := bidQueue; !bidNode.Done(); bidNode.Next() {
		entry := bidNode.GetValue()
		data = append(data, float64(entry.Size))
	}
	askQueue := asks.Entries.Iterate()
	for askNode := askQueue; !askNode.Done(); askNode.Next() {
		entry := askNode.GetValue()
		data = append(data, float64(entry.Size))
	}
	return data
}
