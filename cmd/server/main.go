package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"whaletracker-backend/internal/ingestor"
	"whaletracker-backend/internal/redisclient"
	"whaletracker-backend/internal/signals"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	ctx := context.Background()

	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "redis://localhost:6379" // Use full scheme for local testing now
	}

	client, err := redisclient.NewRedisClient(ctx, redisAddr)
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}
	fmt.Println("Redis connection successful.")

	ingest := ingestor.NewStreamIngestor(client.Rdb)
	go ingest.StartConnect(ctx)

	tracker := signals.NewFlowTracker(client.Rdb)
	go tracker.StartConsumerGroup(ctx)

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status": "healthy"}`))
	})

	r.Get("/api/leaderboard", func(w http.ResponseWriter, r *http.Request) {
		vals, err := client.Rdb.ZRevRange(r.Context(), "flow:largest_trades", 0, -1).Result()
		if err != nil {
			http.Error(w, "Database failure", http.StatusInternalServerError)
			return
		}

		type Trade struct {
			TradeId       string  `json:"trade_id"`
			Asset         string  `json:"asset"`
			Price         float64 `json:"price"`
			Quantity      float64 `json:"quantity"`
			TotalValueUSD float64 `json:"total_value_usd"`
			Timestamp     int64   `json:"timestamp"`
		}

		var trades []Trade
		for _, v := range vals {
			var t Trade
			if err := json.Unmarshal([]byte(v), &t); err == nil {
				trades = append(trades, t)
			}
		}

		sort.Slice(trades, func(i, j int) bool {
			return trades[i].TotalValueUSD > trades[j].TotalValueUSD
		})

		limit := 10
		if len(trades) < limit {
			limit = len(trades)
		}
		top10 := trades[:limit]

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(top10)
	})

	// GET /api/stats/{asset} - Dynamically computes rolling 24h volume metrics from memory buckets
	// GET /api/stats/{asset} - Dynamically computes rolling 24h volume metrics safely
	r.Get("/api/stats/{asset}", func(w http.ResponseWriter, r *http.Request) {
		asset := chi.URLParam(r, "asset")

		// 1. Get last traded execution price snapshot baseline
		hashKey := "stats:asset:" + asset
		baseData, err := client.Rdb.HGetAll(r.Context(), hashKey).Result()

		// Fallback baseline defaults if Redis is completely empty or warming up
		lastPrice := "0.00"
		timestampStr := strconv.FormatInt(time.Now().UnixMilli(), 10)

		// If data actually exists in the hash, extract the real values
		if err == nil && len(baseData) > 0 {
			if price, ok := baseData["last_price"]; ok {
				lastPrice = price
			}
			if ts, ok := baseData["timestamp"]; ok {
				timestampStr = ts
			}
		}

		// 2. Compute true rolling 24-hour volume by aggregating active hourly buckets
		var rollingVolumeUSD float64
		now := time.Now()

		for i := 0; i < 24; i++ {
			hourStr := now.Add(time.Duration(-i) * time.Hour).Format("2006-01-02-15")
			bucketKey := fmt.Sprintf("stats:volume:%s:%s", asset, hourStr)

			valStr, err := client.Rdb.Get(r.Context(), bucketKey).Result()
			if err == nil {
				val, _ := strconv.ParseFloat(valStr, 64)
				rollingVolumeUSD += val
			}
		}

		// 3. Consolidate results into a unified layout response for Next.js to parse
		responsePayload := map[string]string{
			"last_price":            lastPrice,
			"timestamp":             timestampStr,
			"cumulative_volume_usd": strconv.FormatFloat(rollingVolumeUSD, 'f', 2, 64),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(responsePayload)
	})

	r.Get("/api/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Transfer-Encoding", "chunked")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming completely unsupported by client infrastructure", http.StatusInternalServerError)
			return
		}

		pubsub := client.Rdb.Subscribe(r.Context(), "channel:market_events")
		defer pubsub.Close()

		log.Println("Frontend client connected to live SSE data stream pipeline.")
		ch := pubsub.Channel()

		for {
			select {
			case <-r.Context().Done():
				log.Println("Frontend client disconnected from live SSE stream.")
				return

			case msg := <-ch:
				_, err := fmt.Fprintf(w, "data: %s\n\n", msg.Payload)
				if err != nil {
					return
				}
				flusher.Flush()
			}
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("Backend API running on %s\n", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
