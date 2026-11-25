package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/config"
	"github.com/Arman9128/go-backend-engineering/internal/database"
	"github.com/Arman9128/go-backend-engineering/internal/httpapi"
	"github.com/Arman9128/go-backend-engineering/internal/idempotency"
	"github.com/Arman9128/go-backend-engineering/internal/outbox"
	"github.com/Arman9128/go-backend-engineering/internal/queue"
	"github.com/Arman9128/go-backend-engineering/internal/storage"
	"github.com/Arman9128/go-backend-engineering/internal/task"
	"github.com/Arman9128/go-backend-engineering/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Application error:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	db, err := database.NewPostgresDB(cfg)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer db.Close()

	redisClient, err := queue.NewRedisClient(cfg.RedisAddr)
	if err != nil {
		return fmt.Errorf("connect to Redis: %w", err)
	}

	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("close Redis client: %v", err)
		}
	}()

	log.Println("Redis connection established")

	taskStorage := storage.NewPostgresStorage(db)
	taskService := task.NewService("Task Platform API", taskStorage)
	handler := httpapi.NewHandler(taskService, db)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	publisherCtx, cancelPublisher := context.WithCancel(context.Background())
	defer cancelPublisher()

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()

	publisher := outbox.NewPublisher(db, redisClient, "tasks:events")
	taskProcessor := worker.NewTaskProcessor(taskService)
	taskPool := worker.NewPool(
		redisClient,
		taskProcessor,
		"tasks:events",
		"task-workers",
		"worker",
		cfg.WorkerCount,
	)

	idempotencyStore := idempotency.NewPostgresStore(db, 2*time.Minute)
	taskPool.SetIdempotencyStore(idempotencyStore)

	publisherDone := make(chan struct{})
	workerDone := make(chan error, 1)
	serverErrors := make(chan error, 1)

	go func() {
		defer close(publisherDone)
		runOutboxPublisher(publisherCtx, publisher, 500*time.Millisecond)
	}()

	go func() {
		log.Printf("Task worker pool starting: workers=%d", cfg.WorkerCount)
		workerDone <- taskPool.Run(workerCtx)
	}()

	go func() {
		log.Printf("HTTP server listening on %s", cfg.HTTPAddr)
		serverErrors <- server.ListenAndServe()
	}()

	var (
		runErr         error
		workerFinished bool
		serverFinished bool
	)

	select {
	case serverErr := <-serverErrors:
		serverFinished = true
		if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
			runErr = fmt.Errorf("HTTP server: %w", serverErr)
			log.Printf("HTTP server stopped unexpectedly: %v", serverErr)
		}

	case workerErr := <-workerDone:
		workerFinished = true
		if workerErr == nil {
			workerErr = errors.New("worker pool stopped unexpectedly")
		}
		runErr = fmt.Errorf("worker pool stopped unexpectedly: %w", workerErr)
		log.Printf("%v", runErr)

	case <-signalCtx.Done():
		log.Println("Shutdown signal received")
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	if err := server.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(
			runErr,
			fmt.Errorf("shutdown HTTP server: %w", err),
		)
		_ = server.Close()
	}

	cancelShutdown()

	if !serverFinished {
		serverErr := <-serverErrors
		serverFinished = true

		if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
			runErr = errors.Join(
				runErr,
				fmt.Errorf("HTTP server: %w", serverErr),
			)
		}
	}

	cancelPublisher()
	<-publisherDone

	cancelWorker()
	if !workerFinished {
		workerErr := <-workerDone
		workerFinished = true

		if workerErr != nil && !errors.Is(workerErr, context.Canceled) {
			runErr = errors.Join(
				runErr,
				fmt.Errorf("worker pool: %w", workerErr),
			)
		}
	}

	log.Println("Application shutdown complete")

	return runErr
}

func runOutboxPublisher(
	ctx context.Context,
	publisher *outbox.Publisher,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Println("Outbox publisher started")

	for {
		for {
			processed, err := publisher.PublishNext(ctx)

			if err != nil {
				if ctx.Err() == nil {
					log.Printf("Outbox publisher error: %v", err)
				}
				break
			}

			if !processed {
				break
			}
		}

		select {
		case <-ctx.Done():
			log.Println("Outbox publisher stopped")
			return
		case <-ticker.C:
		}
	}
}
