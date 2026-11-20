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

	createdTask, err := taskService.SubmitTask("send-email")
	if err != nil {
		fmt.Println("Application error:", err)
		os.Exit(1)
	}

	fmt.Printf("Created task: ID=%d, Name=%s, Status=%s\n",
		createdTask.ID,
		createdTask.Name,
		createdTask.Status,
	)

	createdTask, err = taskService.SubmitTask("generate-report")
	if err != nil {
		fmt.Println("Application error:", err)
		os.Exit(1)
	}

	fmt.Printf("Created task: ID=%d, Name=%s, Status=%s\n",
		createdTask.ID,
		createdTask.Name,
		createdTask.Status,
	)
}
