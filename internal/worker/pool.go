package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

const defaultShutdownTimeout = 30 * time.Second

type Pool struct {
	queue            MessageQueue
	processor        EventProcessor
	idempotencyStore IdempotencyStore
	stream           string
	group            string
	consumerPrefix   string
	workerCount      int
	shutdownTimeout  time.Duration
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
		queue:           messageQueue,
		processor:       processor,
		stream:          stream,
		group:           group,
		consumerPrefix:  consumerPrefix,
		workerCount:     workerCount,
		shutdownTimeout: defaultShutdownTimeout,
	}
}

func (p *Pool) SetIdempotencyStore(store IdempotencyStore) {
	p.idempotencyStore = store
}

func (p *Pool) SetShutdownTimeout(timeout time.Duration) {
	if timeout > 0 {
		p.shutdownTimeout = timeout
	}
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

	intakeCtx, cancelIntake := context.WithCancel(ctx)
	defer cancelIntake()

	processingCtx, cancelProcessing := context.WithCancel(context.Background())
	defer cancelProcessing()

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

			err := taskWorker.run(intakeCtx, processingCtx)
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

	// Stop fetching new messages while allowing active processing to finish.
	cancelIntake()

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	timeout := p.shutdownTimeout
	if timeout <= 0 {
		timeout = defaultShutdownTimeout
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-workersDone:
		log.Println("Worker pool stopped gracefully")

	case <-timer.C:
		log.Printf("Worker pool shutdown timed out after %s; cancelling active processing", timeout)
		cancelProcessing()

		// Wait for workers to exit before allowing dependent resources to close.
		<-workersDone
	}

	return runErr
}
