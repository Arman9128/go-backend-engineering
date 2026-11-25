package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
)

type Pool struct {
	queue            MessageQueue
	processor        EventProcessor
	idempotencyStore IdempotencyStore
	stream           string
	group            string
	consumerPrefix   string
	workerCount      int
}

func NewPool(
	messageQueue MessageQueue,
	processor EventProcessor,
	stream string,
	group string,
	consumerPrefix string,
	workerCount int,
) *Pool {
	return &Pool{
		queue:          messageQueue,
		processor:      processor,
		stream:         stream,
		group:          group,
		consumerPrefix: consumerPrefix,
		workerCount:    workerCount,
	}
}

func (p *Pool) SetIdempotencyStore(store IdempotencyStore) {
	p.idempotencyStore = store
}

func (p *Pool) Run(ctx context.Context) error {
	if p.queue == nil {
		return fmt.Errorf("worker pool: message queue is nil")
	}

	if p.processor == nil {
		return fmt.Errorf("worker pool: event processor is nil")
	}

	if p.stream == "" || p.group == "" || p.consumerPrefix == "" {
		return fmt.Errorf("worker pool: stream, group and consumer prefix are required")
	}

	if p.workerCount <= 0 {
		return fmt.Errorf("worker pool: worker count must be greater than zero")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errorsCh := make(chan error, p.workerCount)

	var wg sync.WaitGroup

	for i := 1; i <= p.workerCount; i++ {
		consumer := fmt.Sprintf("%s-%d", p.consumerPrefix, i)
		taskWorker := NewWorker(
			p.queue,
			p.processor,
			p.stream,
			p.group,
			consumer,
		)

		if p.idempotencyStore != nil {
			taskWorker.SetIdempotencyStore(p.idempotencyStore)
		}

		wg.Add(1)

		go func(consumer string, taskWorker *Worker) {
			defer wg.Done()

			err := taskWorker.Run(workerCtx)
			if err == nil {
				err = errors.New("worker stopped unexpectedly without an error")
			}

			errorsCh <- fmt.Errorf("consumer %s: %w", consumer, err)
		}(consumer, taskWorker)
	}

	log.Printf(
		"Worker pool started: workers=%d stream=%s group=%s",
		p.workerCount,
		p.stream,
		p.group,
	)

	var runErr error

	select {
	case <-ctx.Done():
		runErr = ctx.Err()

	case err := <-errorsCh:
		if ctx.Err() != nil {
			runErr = ctx.Err()
		} else if errors.Is(err, context.Canceled) {
			runErr = fmt.Errorf("worker pool: worker stopped unexpectedly: %w", err)
		} else {
			runErr = err
		}
	}

	cancel()
	wg.Wait()

	return runErr
}
