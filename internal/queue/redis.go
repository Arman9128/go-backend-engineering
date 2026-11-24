package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(addr string) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     10,
		MinIdleConns: 2,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect to Redis: %w", err)
	}

	return &RedisClient{client: client}, nil
}

func (r *RedisClient) Publish(
	ctx context.Context,
	stream string,
	eventID int64,
	eventType string,
	payload []byte,
) (string, error) {
	if stream == "" {
		return "", fmt.Errorf("publish Redis event: stream is empty")
	}

	if eventID <= 0 {
		return "", fmt.Errorf("publish Redis event: invalid event ID")
	}

	if eventType == "" {
		return "", fmt.Errorf("publish Redis event: event type is empty")
	}

	if len(payload) == 0 {
		return "", fmt.Errorf("publish Redis event: payload is empty")
	}

	entryID, err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"event_id":   eventID,
			"event_type": eventType,
			"payload":    string(payload),
		},
	}).Result()

	if err != nil {
		return "", fmt.Errorf("publish event to Redis Stream: %w", err)
	}

	return entryID, nil
}

func (r *RedisClient) Close() error {
	return r.client.Close()
}
