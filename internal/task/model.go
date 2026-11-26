package task

import (
	"errors"
	"time"
)

var (
	ErrNotFound          = errors.New("task not found")
	ErrInvalidStatus     = errors.New("invalid task status")
	ErrInvalidTransition = errors.New("invalid task status transition")
)

type Task struct {
	ID        int64
	Name      string
	Status    string
	CreatedAt time.Time
	Tags      []string
}

const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusRejected   = "rejected"
)

func IsValidStatus(status string) bool {
	switch status {
	case StatusPending, StatusProcessing, StatusCompleted, StatusFailed, StatusRejected:
		return true
	default:
		return false
	}
}
