package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/queue"
)

type mockMessageQueue struct {
	messages         []queue.Message
	pending          []queue.Message
	acknowledged     []string
	claimCalls       int
	claimNext        string
	claimErr         error
	ackErr           error
	createGroupErr   error
	deadLettered     []string
	deadLetterCalls  int
	deadLetterResult bool
	deadLetterErr    error
}

func (m *mockMessageQueue) CreateConsumerGroup(
	ctx context.Context,
	stream string,
	group string,
) error {
	return m.createGroupErr
}

func (m *mockMessageQueue) ReadGroup(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	count int64,
	block time.Duration,
) ([]queue.Message, error) {
	return m.messages, nil
}

func (m *mockMessageQueue) Ack(
	ctx context.Context,
	stream string,
	group string,
	messageID string,
) error {
	if m.ackErr != nil {
		return m.ackErr
	}

	m.acknowledged = append(m.acknowledged, messageID)
	return nil
}

func (m *mockMessageQueue) MoveToDeadLetter(
	ctx context.Context,
	stream string,
	group string,
	deadLetterStream string,
	message queue.Message,
	maxAttempts int64,
	reason string,
) (bool, error) {
	m.deadLetterCalls++

	if m.deadLetterErr != nil {
		return false, m.deadLetterErr
	}

	if m.deadLetterResult {
		m.deadLettered = append(m.deadLettered, message.ID)
	}

	return m.deadLetterResult, nil
}

func (m *mockMessageQueue) ClaimPending(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	minIdle time.Duration,
	start string,
	count int64,
) ([]queue.Message, string, error) {
	m.claimCalls++

	if m.claimErr != nil {
		return nil, "", m.claimErr
	}

	messages := m.pending
	m.pending = nil

	next := m.claimNext
	if next == "" {
		next = "0-0"
	}

	return messages, next, nil
}

type mockEventProcessor struct {
	err   error
	calls int
}

func (m *mockEventProcessor) Process(
	ctx context.Context,
	message queue.Message,
) error {
	m.calls++
	return m.err
}

func TestWorkerProcessMessagesSuccess(t *testing.T) {
	message := queue.Message{
		ID:      "1-0",
		EventID: 1,
	}

	messageQueue := &mockMessageQueue{}
	processor := &mockEventProcessor{}

	worker := NewWorker(
		messageQueue,
		processor,
		"tasks:events",
		"task-workers",
		"worker-1",
	)

	worker.processMessages(context.Background(), []queue.Message{message})

	if processor.calls != 1 {
		t.Fatalf("processor calls = %d, want 1", processor.calls)
	}

	if len(messageQueue.acknowledged) != 1 {
		t.Fatalf("acknowledged messages = %d, want 1", len(messageQueue.acknowledged))
	}

	if messageQueue.acknowledged[0] != message.ID {
		t.Fatalf("acknowledged ID = %q, want %q", messageQueue.acknowledged[0], message.ID)
	}
}

func TestWorkerProcessMessagesErrorDoesNotAck(t *testing.T) {
	message := queue.Message{
		ID:      "2-0",
		EventID: 2,
	}

	messageQueue := &mockMessageQueue{}
	processor := &mockEventProcessor{
		err: errors.New("processing failed"),
	}

	worker := NewWorker(
		messageQueue,
		processor,
		"tasks:events",
		"task-workers",
		"worker-1",
	)

	worker.processMessages(context.Background(), []queue.Message{message})

	if processor.calls != 1 {
		t.Fatalf("processor calls = %d, want 1", processor.calls)
	}

	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("acknowledged messages = %d, want 0", len(messageQueue.acknowledged))
	}

	if messageQueue.deadLetterCalls != 1 {
		t.Fatalf("MoveToDeadLetter() calls = %d, want 1", messageQueue.deadLetterCalls)
	}
}

func TestWorkerMovesMessageToDeadLetter(t *testing.T) {
	message := queue.Message{
		ID:      "4-0",
		EventID: 4,
	}

	messageQueue := &mockMessageQueue{
		deadLetterResult: true,
	}
	processor := &mockEventProcessor{
		err: errors.New("processing failed"),
	}

	worker := NewWorker(
		messageQueue,
		processor,
		"tasks:events",
		"task-workers",
		"worker-1",
	)

	worker.processMessages(context.Background(), []queue.Message{message})

	if messageQueue.deadLetterCalls != 1 {
		t.Fatalf("MoveToDeadLetter() calls = %d, want 1", messageQueue.deadLetterCalls)
	}

	if len(messageQueue.deadLettered) != 1 ||
		messageQueue.deadLettered[0] != message.ID {
		t.Fatalf("dead-lettered messages = %v, want [%s]",
			messageQueue.deadLettered, message.ID)
	}

	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("ACK calls = %d, want 0; DLQ operation handles ACK atomically",
			len(messageQueue.acknowledged))
	}
}

func TestWorkerDeadLetterErrorDoesNotAck(t *testing.T) {
	message := queue.Message{
		ID:      "5-0",
		EventID: 5,
	}

	messageQueue := &mockMessageQueue{
		deadLetterErr: errors.New("Redis unavailable"),
	}
	processor := &mockEventProcessor{
		err: errors.New("processing failed"),
	}

	worker := NewWorker(
		messageQueue,
		processor,
		"tasks:events",
		"task-workers",
		"worker-1",
	)

	worker.processMessages(context.Background(), []queue.Message{message})

	if messageQueue.deadLetterCalls != 1 {
		t.Fatalf("MoveToDeadLetter() calls = %d, want 1", messageQueue.deadLetterCalls)
	}

	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("ACK calls = %d, want 0", len(messageQueue.acknowledged))
	}

	if len(messageQueue.deadLettered) != 0 {
		t.Fatalf("dead-lettered messages = %v, want none", messageQueue.deadLettered)
	}
}

func TestWorkerRecoverPending(t *testing.T) {
	message := queue.Message{
		ID:      "3-0",
		EventID: 3,
	}

	messageQueue := &mockMessageQueue{
		pending: []queue.Message{message},
	}

	processor := &mockEventProcessor{}

	worker := NewWorker(
		messageQueue,
		processor,
		"tasks:events",
		"task-workers",
		"worker-1",
	)

	if err := worker.recoverPending(context.Background()); err != nil {
		t.Fatalf("recoverPending() error = %v", err)
	}

	if messageQueue.claimCalls != 1 {
		t.Fatalf("ClaimPending() calls = %d, want 1", messageQueue.claimCalls)
	}

	if processor.calls != 1 {
		t.Fatalf("processor calls = %d, want 1", processor.calls)
	}

	if len(messageQueue.acknowledged) != 1 ||
		messageQueue.acknowledged[0] != message.ID {
		t.Fatalf("acknowledged messages = %v, want [%s]",
			messageQueue.acknowledged, message.ID)
	}
}
