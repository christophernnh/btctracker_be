package redisclient

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	Rdb *redis.Client
}


// Passes context and address as parameters
func NewRedisClient(ctx context.Context, connectionURL string) (*Client, error) {
	// Use ParseURL instead of manual Addr/Password configuration strings
	opts, err := redis.ParseURL(connectionURL)
	if err != nil {
		return nil, err
	}

	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &Client{Rdb: rdb}, nil
}