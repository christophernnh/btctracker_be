package signals

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// MarketUpdatePayload - structure to broadcast over Pub/Sub for frontend to consume
type MarketUpdatePayload struct {
	TradeId       string  `json:"trade_id"`
	Asset         string  `json:"asset"`
	Price         float64 `json:"price"`
	Quantity      float64 `json:"quantity"`
	TotalValueUSD float64 `json:"total_value_usd"`
	Timestamp     int64   `json:"timestamp"`
}

type FlowTracker struct {
	rdb *redis.Client
}

func NewFlowTracker(rdb *redis.Client) *FlowTracker {
	return &FlowTracker{rdb: rdb}
}

// StartConsumerGroup initializes the consumer group and processes incoming stream data infinitely
func (ft *FlowTracker) StartConsumerGroup(ctx context.Context) {
	streamKey := "flow:stream"
	groupName := "whale_processors"
	consumerName := "worker_node_1"

	// 1. Create the Consumer Group if it doesn't already exist
	err := ft.rdb.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err != nil {
		log.Printf("Consumer group notice: %v", err)
	}

	for {
		// 2. Read new unread messages dedicated to this group
		streams, err := ft.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
			Count:    50,              
			Block:    5 * time.Second, 
		}).Result()

		if err != nil && err != redis.Nil {
			log.Printf("❌ Consumer error reading stream: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				ft.processStreamMessage(ctx, message, groupName, streamKey)
			}
		}
	}
}

// processStreamMessage extracts values, updates metrics, and acknowledges the event
func (ft *FlowTracker) processStreamMessage(ctx context.Context, msg redis.XMessage, groupName, streamKey string) {
	tradeID, _ := msg.Values["trade_id"].(string)
	asset, _ := msg.Values["asset"].(string)
	priceStr, _ := msg.Values["price"].(string)
	qtyStr, _ := msg.Values["quantity"].(string)
	tsStr, _ := msg.Values["timestamp"].(string)

	price, _ := strconv.ParseFloat(priceStr, 64)
	qty, _ := strconv.ParseFloat(qtyStr, 64)
	timestamp, _ := strconv.ParseInt(tsStr, 10, 64)

	totalValueUSD := price * qty

	// 1. UPDATE FEATURE A: 24h Sliding Window Leaderboard with GATEKEEPER Filter
	// Only track trades worth $10,000 USD or more to preserve memory and speed
	if totalValueUSD >= 10000.0 {
		leaderboardKey := "flow:largest_trades"
		
		tradeSnapshot := map[string]interface{}{
			"trade_id":        tradeID,
			"asset":           asset,
			"price":           price,
			"quantity":        qty,
			"total_value_usd": totalValueUSD,
			"timestamp":       timestamp,
		}
		
		tradeJSON, err := json.Marshal(tradeSnapshot)
		if err == nil {
			err = ft.rdb.ZAdd(ctx, leaderboardKey, redis.Z{
				Score:  float64(timestamp), // Timestamp is score for window pruning
				Member: string(tradeJSON),
			}).Err()
			if err != nil {
				log.Printf("❌ Failed to log trade to leaderboard: %v", err)
			}
		}

		// THE 24H PRUNER: Drop any historical leaderboard metrics older than 24h
		nowMilli := time.Now().UnixMilli()
		twentyFourHoursAgo := nowMilli - (24 * 60 * 60 * 1000)
		ft.rdb.ZRemRangeByScore(ctx, leaderboardKey, "-inf", strconv.FormatInt(twentyFourHoursAgo, 10))
	}

	// 2. UPDATE FEATURE B: Real-Time Price Baseline & Rolling Hourly Volume Buckets
	hashKey := "stats:asset:" + asset
	err := ft.rdb.HSet(ctx, hashKey, map[string]interface{}{
		"last_price": priceStr,
		"timestamp":  tsStr,
	}).Err()
	if err != nil {
		log.Printf("❌ Failed to update baseline stats: %v", err)
	}

	// Dynamic hourly volume aggregation key
	currentHourStr := time.Now().Format("2006-01-02-15") 
	hourlyVolumeKey := "stats:volume:" + asset + ":" + currentHourStr

	ft.rdb.IncrByFloat(ctx, hourlyVolumeKey, totalValueUSD)
	ft.rdb.Expire(ctx, hourlyVolumeKey, 25*time.Hour) // Self-destruct bucket in 25 hours

	// 3. UPDATE FEATURE C: Pub-Sub (Realtime Broadcast)
	payload := MarketUpdatePayload{
		TradeId:       tradeID,
		Asset:         asset,
		Price:         price,
		Quantity:      qty,
		TotalValueUSD: totalValueUSD,
		Timestamp:     timestamp,
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("❌ Failed to marshal update payload: %v", err)
	} else {
		ft.rdb.Publish(ctx, "channel:market_events", jsonPayload)
	}

	// Whales alerting matrix
	whaleThreshold := 50000.0
	if totalValueUSD >= whaleThreshold {
		log.Printf("🚨 WHALE SWEEP DETECTED: %s generated $%.2f volume on %s", tradeID, totalValueUSD, asset)
		ft.rdb.Publish(ctx, "channel:whale_alerts", "Whale activity detected!")
	}

	ft.rdb.XAck(ctx, streamKey, groupName, msg.ID)
}