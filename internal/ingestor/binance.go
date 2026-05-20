package ingestor

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// BinanceAggTrade matches the JSON payload from the Binance stream
type BinanceAggTrade struct {
	AggregateTradeID int64  `json:"a"` // aggregate trade id
	EventType        string `json:"e"` // eventtype
	EventTime        int64  `json:"E"` // eventtime
	Symbol           string `json:"s"` // symbol
	Price            string `json:"p"` // price
	Quantity         string `json:"q"` // quantity
}

type StreamIngestor struct {
	rdb *redis.Client
}

func NewStreamIngestor(rdb *redis.Client) *StreamIngestor {
	return &StreamIngestor{rdb: rdb}
}

func (si *StreamIngestor) StartConnect(ctx context.Context) {
	// Binance market data endpoint, defaults to standard secure port 443
	url := "wss://data-stream.binance.vision/ws/ethusdt@aggTrade"

	log.Printf("Connecting to Official Binance Market Data: %s", url)

	// Set a timeout
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		log.Printf("Handshake failed: %v. Retrying in 5s...", err)
		time.Sleep(5 * time.Second)
		go si.StartConnect(ctx) // Retry recursively
		return
	}
	defer conn.Close()

	log.Println("Websocket hadshake successful.")

	for {
		// ReadMessage automatically handles Ping/Pong control frames behind the scenes
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Stream interrupted: %v. Reconnecting...", err)
			time.Sleep(5 * time.Second)
			go si.StartConnect(ctx)
			return
		}

		var rawTrade BinanceAggTrade
		if err := json.Unmarshal(message, &rawTrade); err != nil {
			// Log error if parsing fails, but keep the stream alive
			log.Printf("JSON Parse Error: %v | Raw Message was: %s", err, string(message))
			continue
		}

		// Push to Redis Streams
		err = si.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "flow:stream",
			MaxLen: 10000,
			Approx: true,
			Values: map[string]interface{}{
				"trade_id":  rawTrade.AggregateTradeID,
				"asset":     rawTrade.Symbol,
				"price":     rawTrade.Price,
				"quantity":  rawTrade.Quantity,
				"timestamp": rawTrade.EventTime,
			},
		}).Err()

		if err != nil {
			log.Printf("❌ Redis write error: %v", err)
		}
	}
}
