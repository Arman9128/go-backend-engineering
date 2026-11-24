package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/queue"
)

type MessageQueue interface {
	CreateConsumerGroup(
		ctx context.Context,
		stream string,
		group string,
	) error

	ReadGroup(
		ctx context.Context,
		stream string,
		group string,
		consumer string,
		count int64,
		block time.Duration,
	) ([]queue.Message, error)

	Ack(
		ctx context.Context,
		stream string,
		group string,
		messageID string,
	) error

	MoveToDeadLetter(
		ctx context.Context,
		stream string,
		group string,
		deadLetterStream string,
		message queue.Message,
		maxAttempts int64,
		reason string,
	) (bool, error)

	ClaimPending(
		ctx context.Context,
		stream string,
		group string,
		consumer string,
		minIdle time.Duration,
		start string,
		count int64,
	) ([]queue.Message, string, error)
}

type Worker struct {
	queue     MessageQueue
	processor EventProcessor

	stream           string
	group            string
	consumer         string
	deadLetterStream string
	maxAttempts      int64
	batchSize        int64
	blockDuration    time.Duration
	claimMinIdle     time.Duration
}

func NewWorker(
	messageQueue MessageQueue,
	processor EventProcessor,
	stream string,
	group string,
	consumer string,
) *Worker {
	return &Worker{
		queue:            messageQueue,
		processor:        processor,
		stream:           stream,
		group:            group,
		consumer:         consumer,
		deadLetterStream: stream + ":dead",
		maxAttempts:      5,
		batchSize:        10,
		blockDuration:    2 * time.Second,
		claimMinIdle:     30 * time.Second,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if w.queue == nil {
		return fmt.Errorf("worker: message queue is nil")
	}

	if w.processor == nil {
		return fmt.Errorf("worker: event processor is nil")
	}

	if w.stream == "" || w.group == "" || w.consumer == "" {
		return fmt.Errorf("worker: stream, group and consumer are required")
	}

	if err := w.queue.CreateConsumerGroup(ctx, w.stream, w.group); err != nil {
		return fmt.Errorf("worker: create consumer group: %w", err)
	}

	log.Printf(
		"Worker started: stream=%s group=%s consumer=%s",
		w.stream,
		w.group,
		w.consumer,
	)

	for {
		if err := ctx.Err(); err != nil {
			log.Println("Worker stopped")
			return err
		}

		if err := w.recoverPending(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			log.Printf("Worker pending recovery error: %v", err)

			if !wait(ctx, time.Second) {
				return ctx.Err()
			}

			continue
		}

		messages, err := w.queue.ReadGroup(
			ctx,
			w.stream,
			w.group,
			w.consumer,
			w.batchSize,
			w.blockDuration,
		)

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			log.Printf("Worker read error: %v", err)

			if !wait(ctx, time.Second) {
				return ctx.Err()
			}

			continue
		}

		w.processMessages(ctx, messages)
	}
}

func (w *Worker) recoverPending(ctx context.Context) error {
	start := "0-0"

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		messages, next, err := w.queue.ClaimPending(
			ctx,
			w.stream,
			w.group,
			w.consumer,
			w.claimMinIdle,
			start,
			w.batchSize,
		)
		if err != nil {
			return err
		}

		w.processMessages(ctx, messages)

		if next == "" || next == "0-0" {
			return nil
		}

		start = next
	}
}

func (w *Worker) processMessages(ctx context.Context, messages []queue.Message) {
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			return
		}

		if err := w.processor.Process(ctx, message); err != nil {
			log.Printf(
				"Worker processing failed: message_id=%s event_id=%d error=%v",
				message.ID,
				message.EventID,
				err,
			)

			moved, dlqErr := w.queue.MoveToDeadLetter(
				ctx,
				w.stream,
				w.group,
				w.deadLetterStream,
				message,
				w.maxAttempts,
				err.Error(),
			)
			if dlqErr != nil {
				log.Printf(
					"Worker dead-letter move failed: message_id=%s error=%v",
					message.ID,
					dlqErr,
				)
				continue
			}

			if moved {
				log.Printf(
					"Worker message moved to dead-letter stream: message_id=%s event_id=%d",
					message.ID,
					message.EventID,
				)
			}
			continue
		}

		if err := w.queue.Ack(ctx, w.stream, w.group, message.ID); err != nil {
			log.Printf(
				"Worker ACK failed: message_id=%s error=%v",
				message.ID,
				err,
			)
			continue
		}

		log.Printf(
			"Worker message processed: message_id=%s event_id=%d",
			message.ID,
			message.EventID,
		)
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
