package main

import (
	"fmt"
	"os"

	"github.com/Arman9128/go-backend-engineering/internal/storage"
	"github.com/Arman9128/go-backend-engineering/internal/task"
)

func main() {
	taskStorage := storage.NewMemoryStorage()
	taskService := task.NewService("Task Platform API", taskStorage)

	if err := taskService.SubmitTask("send-email"); err != nil {
		fmt.Println("Application error:", err)
		os.Exit(1)
	}

	if err := taskService.SubmitTask("generate-report"); err != nil {
		fmt.Println("Application error:", err)
		os.Exit(1)
	}
}
