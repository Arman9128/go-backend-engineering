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

type IdempotencyStore interface {
	Acquire(ctx context.Context, eventID int64) (token string, acquired bool, completed bool, err error)
	Renew(ctx context.Context, eventID int64, token string) error
	Complete(ctx context.Context, eventID int64, token string) error
	Release(ctx context.Context, eventID int64, token string, processingErr error) error
}

type Worker struct {
	queue            MessageQueue
	processor        EventProcessor
	idempotencyStore IdempotencyStore

	stream             string
	group              string
	consumer           string
	deadLetterStream   string
	maxAttempts        int64
	batchSize          int64
	blockDuration      time.Duration
	claimMinIdle       time.Duration
	leaseRenewInterval time.Duration
}

func NewWorker(
	messageQueue MessageQueue,
	processor EventProcessor,
	stream string,
	group string,
	consumer string,
) *Worker {
	return &Worker{
		queue:              messageQueue,
		processor:          processor,
		stream:             stream,
		group:              group,
		consumer:           consumer,
		deadLetterStream:   stream + ":dead",
		maxAttempts:        5,
		batchSize:          10,
		blockDuration:      2 * time.Second,
		claimMinIdle:       30 * time.Second,
		leaseRenewInterval: 30 * time.Second,
	}
}

func (w *Worker) SetIdempotencyStore(store IdempotencyStore) {
	w.idempotencyStore = store
}

func (w *Worker) SetLeaseRenewInterval(interval time.Duration) {
	if interval > 0 {
		w.leaseRenewInterval = interval
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

		var lockToken string

		if w.idempotencyStore != nil {
			token, acquired, completed, err := w.idempotencyStore.Acquire(
				ctx,
				message.EventID,
			)
			if err != nil {
				log.Printf(
					"Worker idempotency acquire failed: message_id=%s event_id=%d error=%v",
					message.ID,
					message.EventID,
					err,
				)
				continue
			}

			if completed {
				if err := w.queue.Ack(ctx, w.stream, w.group, message.ID); err != nil {
					log.Printf(
						"Worker duplicate ACK failed: message_id=%s event_id=%d error=%v",
						message.ID,
						message.EventID,
						err,
					)
					continue
				}

				log.Printf(
					"Worker duplicate event acknowledged: message_id=%s event_id=%d",
					message.ID,
					message.EventID,
				)
				continue
			}

			if !acquired {
				log.Printf(
					"Worker event is already being processed: message_id=%s event_id=%d",
					message.ID,
					message.EventID,
				)
				continue
			}

			lockToken = token
		}

		var processingErr error

		if w.idempotencyStore != nil {
			var heartbeatErr error
			processingErr, heartbeatErr = w.processWithHeartbeat(
				ctx,
				message,
				lockToken,
			)
			if heartbeatErr != nil {
				log.Printf(
					"Worker idempotency lease renewal failed: message_id=%s event_id=%d error=%v",
					message.ID,
					message.EventID,
					heartbeatErr,
				)
				continue
			}
		} else {
			processingErr = w.processor.Process(ctx, message)
		}

		if processingErr != nil {
			log.Printf(
				"Worker processing failed: message_id=%s event_id=%d error=%v",
				message.ID,
				message.EventID,
				processingErr,
			)

			if w.idempotencyStore != nil {
				if err := w.idempotencyStore.Release(
					ctx,
					message.EventID,
					lockToken,
					processingErr,
				); err != nil {
					log.Printf(
						"Worker idempotency release failed: message_id=%s event_id=%d error=%v",
						message.ID,
						message.EventID,
						err,
					)
					continue
				}
			}

			moved, dlqErr := w.queue.MoveToDeadLetter(
				ctx,
				w.stream,
				w.group,
				w.deadLetterStream,
				message,
				w.maxAttempts,
				processingErr.Error(),
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

		if w.idempotencyStore != nil {
			if err := w.idempotencyStore.Complete(
				ctx,
				message.EventID,
				lockToken,
			); err != nil {
				log.Printf(
					"Worker idempotency completion failed: message_id=%s event_id=%d error=%v",
					message.ID,
					message.EventID,
					err,
				)
				continue
			}
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

func (w *Worker) processWithHeartbeat(
	ctx context.Context,
	message queue.Message,
	token string,
) (error, error) {
	processCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	interval := w.leaseRenewInterval
	if interval <= 0 {
		return w.processor.Process(processCtx, message), nil
	}

	stop := make(chan struct{})
	heartbeatDone := make(chan error, 1)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				heartbeatDone <- nil
				return
			case <-processCtx.Done():
				heartbeatDone <- nil
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(
					processCtx,
					interval/2,
				)
				err := w.idempotencyStore.Renew(
					renewCtx,
					message.EventID,
					token,
				)
				renewCancel()

				if err != nil {
					heartbeatDone <- err
					cancel()
					return
				}
			}
		}
	}()

	processingErr := w.processor.Process(processCtx, message)
	close(stop)

	heartbeatErr := <-heartbeatDone
	return processingErr, heartbeatErr
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
