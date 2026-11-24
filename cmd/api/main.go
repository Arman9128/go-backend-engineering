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

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	workerCtx, cancelWorker := context.WithCancel(ctx)
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

	publisherDone := make(chan struct{})
	workerDone := make(chan error, 1)

	go func() {
		defer close(publisherDone)
		runOutboxPublisher(workerCtx, publisher, 500*time.Millisecond)
	}()

	go func() {
		log.Printf("Task worker pool starting: workers=%d", cfg.WorkerCount)
		workerDone <- taskPool.Run(workerCtx)
	}()

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf("HTTP server listening on %s", cfg.HTTPAddr)
		serverErrors <- server.ListenAndServe()
	}()

	var serverErr error
	serverFinished := false
	var workerErr error
	workerFinished := false

	select {
	case serverErr = <-serverErrors:
		serverFinished = true
		stop()

	case workerErr = <-workerDone:
		workerFinished = true

		if ctx.Err() == nil {
			if workerErr == nil {
				workerErr = errors.New("worker pool stopped unexpectedly")
			}

			log.Printf("Task worker pool stopped unexpectedly: %v", workerErr)
			stop()
		}

	case <-ctx.Done():
		log.Println("Shutdown signal received")
	}

	if serverFinished {
		cancelWorker()
		<-publisherDone

		if !workerFinished {
			workerErr = <-workerDone
		}

		if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server: %w", serverErr)
		}

		if workerErr != nil && !errors.Is(workerErr, context.Canceled) {
			return fmt.Errorf("worker pool: %w", workerErr)
		}

		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	if err := server.Shutdown(shutdownCtx); err != nil {
		cancel()
		cancelWorker()

		<-publisherDone

		if !workerFinished {
			workerErr = <-workerDone
		}

		_ = server.Close()

		if !serverFinished {
			serverErr = <-serverErrors
		}

		return fmt.Errorf("shutdown HTTP server: %w", err)
	}

	cancel()
	cancelWorker()

	<-publisherDone

	if !workerFinished {
		workerErr = <-workerDone
	}

	if !serverFinished {
		serverErr = <-serverErrors
	}

	if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server: %w", serverErr)
	}

	if workerErr != nil && !errors.Is(workerErr, context.Canceled) {
		return fmt.Errorf("worker pool: %w", workerErr)
	}

	log.Println("HTTP server stopped gracefully")

	return nil
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
