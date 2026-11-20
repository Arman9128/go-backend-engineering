package task

import "time"

type Task struct {
	ID        int64
	Name      string
	Status    string
	CreatedAt time.Time
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
