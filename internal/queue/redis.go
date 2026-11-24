package queue

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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

func (r *RedisClient) CreateConsumerGroup(
	ctx context.Context,
	stream string,
	group string,
) error {
	if stream == "" {
		return fmt.Errorf("create Redis consumer group: stream is empty")
	}

	if group == "" {
		return fmt.Errorf("create Redis consumer group: group is empty")
	}

	err := r.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create Redis consumer group: %w", err)
	}

	return nil
}

func (r *RedisClient) ReadGroup(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	count int64,
	block time.Duration,
) ([]Message, error) {
	if stream == "" || group == "" || consumer == "" {
		return nil, fmt.Errorf("read Redis consumer group: stream, group and consumer are required")
	}

	if count <= 0 {
		return nil, fmt.Errorf("read Redis consumer group: count must be positive")
	}

	if block < 0 {
		return nil, fmt.Errorf("read Redis consumer group: block cannot be negative")
	}

	streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}

		return nil, fmt.Errorf("read Redis consumer group: %w", err)
	}

	var entries []redis.XMessage

	for _, result := range streams {
		entries = append(entries, result.Messages...)
	}

	messages, err := parseRedisMessages(entries)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func parseRedisMessages(entries []redis.XMessage) ([]Message, error) {
	messages := make([]Message, 0, len(entries))

	for _, entry := range entries {
		eventID, err := redisMessageInt64(entry.Values["event_id"])
		if err != nil {
			return nil, fmt.Errorf("parse Redis message %s event_id: %w", entry.ID, err)
		}

		eventType, ok := entry.Values["event_type"].(string)
		if !ok || eventType == "" {
			return nil, fmt.Errorf("parse Redis message %s: invalid event_type", entry.ID)
		}

		payload, ok := entry.Values["payload"].(string)
		if !ok || payload == "" {
			return nil, fmt.Errorf("parse Redis message %s: invalid payload", entry.ID)
		}

		messages = append(messages, Message{
			ID:        entry.ID,
			EventID:   eventID,
			EventType: eventType,
			Payload:   []byte(payload),
		})
	}

	return messages, nil
}

func (r *RedisClient) Ack(
	ctx context.Context,
	stream string,
	group string,
	messageID string,
) error {
	if stream == "" || group == "" || messageID == "" {
		return fmt.Errorf("acknowledge Redis message: stream, group and message ID are required")
	}

	if err := r.client.XAck(ctx, stream, group, messageID).Err(); err != nil {
		return fmt.Errorf("acknowledge Redis message %s: %w", messageID, err)
	}

	return nil
}

func redisMessageInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case string:
		result, err := strconv.ParseInt(v, 10, 64)
		if err != nil || result <= 0 {
			return 0, fmt.Errorf("invalid event ID %q", v)
		}
		return result, nil
	case int64:
		if v <= 0 {
			return 0, fmt.Errorf("invalid event ID %d", v)
		}
		return v, nil
	default:
		return 0, fmt.Errorf("unexpected event ID type %T", value)
	}
}

func (r *RedisClient) ClaimPending(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	minIdle time.Duration,
	start string,
	count int64,
) ([]Message, string, error) {
	if stream == "" || group == "" || consumer == "" {
		return nil, "", fmt.Errorf("claim pending messages: stream, group and consumer are required")
	}

	if minIdle < 0 {
		return nil, "", fmt.Errorf("claim pending messages: minimum idle time cannot be negative")
	}

	if start == "" {
		start = "0-0"
	}

	if count <= 0 {
		return nil, "", fmt.Errorf("claim pending messages: count must be positive")
	}

	entries, next, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()
	if err != nil {
		return nil, "", fmt.Errorf("claim pending messages: %w", err)
	}

	messages, err := parseRedisMessages(entries)
	if err != nil {
		return nil, next, err
	}

	return messages, next, nil
}

func (r *RedisClient) GetDeliveryCount(
	ctx context.Context,
	stream string,
	group string,
	messageID string,
) (int64, error) {
	if stream == "" || group == "" || messageID == "" {
		return 0, fmt.Errorf("get delivery count: stream, group and message ID are required")
	}

	entries, err := r.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  messageID,
		End:    messageID,
		Count:  1,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("get delivery count for message %s: %w", messageID, err)
	}

	if len(entries) == 0 {
		return 0, fmt.Errorf("get delivery count: message %s is not pending", messageID)
	}

	return entries[0].RetryCount, nil
}

func (r *RedisClient) MoveToDeadLetter(
	ctx context.Context,
	stream string,
	group string,
	deadLetterStream string,
	message Message,
	maxAttempts int64,
	reason string,
) (bool, error) {
	if stream == "" || group == "" || deadLetterStream == "" || message.ID == "" {
		return false, fmt.Errorf("move message to dead letter: stream, group, dead letter stream and message ID are required")
	}

	if maxAttempts <= 0 {
		return false, fmt.Errorf("move message to dead letter: max attempts must be positive")
	}

	if reason == "" {
		return false, fmt.Errorf("move message to dead letter: reason is required")
	}

	script := redis.NewScript(`
		local pending = redis.call(
			'XPENDING',
			KEYS[1],
			ARGV[1],
			ARGV[2],
			ARGV[2],
			1
		)

		if #pending == 0 then
			return redis.error_reply('message is not pending')
		end

		local deliveries = tonumber(pending[1][4])

		if deliveries < tonumber(ARGV[3]) then
			return 0
		end

		redis.call(
			'XADD',
			KEYS[2],
			'*',
			'original_message_id', ARGV[2],
			'event_id', ARGV[4],
			'event_type', ARGV[5],
			'payload', ARGV[6],
			'attempts', tostring(deliveries),
			'reason', ARGV[7],
			'failed_at', ARGV[8]
		)

		local acknowledged = redis.call(
			'XACK',
			KEYS[1],
			ARGV[1],
			ARGV[2]
		)

		if acknowledged ~= 1 then
			return redis.error_reply('failed to acknowledge original message')
		end

		return 1
	`)

	result, err := script.Run(
		ctx,
		r.client,
		[]string{stream, deadLetterStream},
		group,
		message.ID,
		maxAttempts,
		message.EventID,
		message.EventType,
		string(message.Payload),
		reason,
		time.Now().UTC().Format(time.RFC3339Nano),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("move message %s to dead letter stream: %w", message.ID, err)
	}

	return result == 1, nil
}
