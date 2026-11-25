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

type intakeShutdownTestProcessor struct {
	started chan struct{}
	release chan struct{}
	calls   int
}

func (p *intakeShutdownTestProcessor) Process(
	ctx context.Context,
	message queue.Message,
) error {
	p.calls++
	if p.calls == 1 {
		close(p.started)
	}

	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestWorkerFinishesActiveMessageAfterIntakeCancellation(t *testing.T) {
	messages := []queue.Message{
		{ID: "30-0", EventID: 30},
		{ID: "31-0", EventID: 31},
	}

	messageQueue := &mockMessageQueue{}
	processor := &intakeShutdownTestProcessor{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	w := NewWorker(messageQueue, processor, "tasks:events", "task-workers", "worker-1")

	intakeCtx, cancelIntake := context.WithCancel(context.Background())
	defer cancelIntake()

	processingCtx, cancelProcessing := context.WithCancel(context.Background())
	defer cancelProcessing()

	done := make(chan struct{})
	go func() {
		w.processMessagesWithContexts(intakeCtx, processingCtx, messages)
		close(done)
	}()

	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("processor did not start")
	}

	cancelIntake()
	close(processor.release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not finish active message")
	}

	if processor.calls != 1 {
		t.Fatalf("processor calls = %d, want 1", processor.calls)
	}

	if len(messageQueue.acknowledged) != 1 ||
		messageQueue.acknowledged[0] != messages[0].ID {
		t.Fatalf("acknowledged = %v, want [%s]", messageQueue.acknowledged, messages[0].ID)
	}
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

type mockIdempotencyStore struct {
	token         string
	acquired      bool
	completed     bool
	acquireErr    error
	renewErr      error
	completeErr   error
	releaseErr    error
	acquireCalls  int
	renewCalls    int
	renewSignal   chan struct{}
	completeCalls int
	releaseCalls  int
	releasedErr   error
}

func (m *mockIdempotencyStore) Acquire(
	ctx context.Context,
	eventID int64,
) (string, bool, bool, error) {
	m.acquireCalls++
	return m.token, m.acquired, m.completed, m.acquireErr
}

func (m *mockIdempotencyStore) Renew(
	ctx context.Context,
	eventID int64,
	token string,
) error {
	m.renewCalls++
	if m.renewSignal != nil {
		select {
		case m.renewSignal <- struct{}{}:
		default:
		}
	}
	return m.renewErr
}

func (m *mockIdempotencyStore) Complete(
	ctx context.Context,
	eventID int64,
	token string,
) error {
	m.completeCalls++
	return m.completeErr
}

func (m *mockIdempotencyStore) Release(
	ctx context.Context,
	eventID int64,
	token string,
	processingErr error,
) error {
	m.releaseCalls++
	m.releasedErr = processingErr
	return m.releaseErr
}

func newIdempotentTestWorker(
	messageQueue *mockMessageQueue,
	processor *mockEventProcessor,
	store *mockIdempotencyStore,
) *Worker {
	w := NewWorker(
		messageQueue,
		processor,
		"tasks:events",
		"task-workers",
		"worker-1",
	)
	w.SetIdempotencyStore(store)
	return w
}

func TestWorkerIdempotencyCompletedEventIsAcknowledged(t *testing.T) {
	message := queue.Message{ID: "10-0", EventID: 10}
	messageQueue := &mockMessageQueue{}
	processor := &mockEventProcessor{}
	store := &mockIdempotencyStore{completed: true}

	w := newIdempotentTestWorker(messageQueue, processor, store)
	w.processMessages(context.Background(), []queue.Message{message})

	if processor.calls != 0 {
		t.Fatalf("processor calls = %d, want 0", processor.calls)
	}
	if store.acquireCalls != 1 {
		t.Fatalf("Acquire calls = %d, want 1", store.acquireCalls)
	}
	if len(messageQueue.acknowledged) != 1 ||
		messageQueue.acknowledged[0] != message.ID {
		t.Fatalf("acknowledged = %v, want [%s]", messageQueue.acknowledged, message.ID)
	}
	if messageQueue.deadLetterCalls != 0 {
		t.Fatalf("dead-letter calls = %d, want 0", messageQueue.deadLetterCalls)
	}
}

func TestWorkerIdempotencyBusyEventRemainsPending(t *testing.T) {
	message := queue.Message{ID: "11-0", EventID: 11}
	messageQueue := &mockMessageQueue{}
	processor := &mockEventProcessor{}
	store := &mockIdempotencyStore{acquired: false}

	w := newIdempotentTestWorker(messageQueue, processor, store)
	w.processMessages(context.Background(), []queue.Message{message})

	if processor.calls != 0 {
		t.Fatalf("processor calls = %d, want 0", processor.calls)
	}
	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("ACK calls = %d, want 0", len(messageQueue.acknowledged))
	}
	if messageQueue.deadLetterCalls != 0 {
		t.Fatalf("dead-letter calls = %d, want 0", messageQueue.deadLetterCalls)
	}
}

func TestWorkerIdempotencyAcquireErrorLeavesMessagePending(t *testing.T) {
	message := queue.Message{ID: "12-0", EventID: 12}
	messageQueue := &mockMessageQueue{}
	processor := &mockEventProcessor{}
	store := &mockIdempotencyStore{
		acquireErr: errors.New("PostgreSQL unavailable"),
	}

	w := newIdempotentTestWorker(messageQueue, processor, store)
	w.processMessages(context.Background(), []queue.Message{message})

	if processor.calls != 0 {
		t.Fatalf("processor calls = %d, want 0", processor.calls)
	}
	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("ACK calls = %d, want 0", len(messageQueue.acknowledged))
	}
	if messageQueue.deadLetterCalls != 0 {
		t.Fatalf("dead-letter calls = %d, want 0", messageQueue.deadLetterCalls)
	}
}

func TestWorkerIdempotencyCompletesBeforeAck(t *testing.T) {
	message := queue.Message{ID: "13-0", EventID: 13}
	messageQueue := &mockMessageQueue{}
	processor := &mockEventProcessor{}
	store := &mockIdempotencyStore{
		token:    "test-token",
		acquired: true,
	}

	w := newIdempotentTestWorker(messageQueue, processor, store)
	w.processMessages(context.Background(), []queue.Message{message})

	if processor.calls != 1 {
		t.Fatalf("processor calls = %d, want 1", processor.calls)
	}
	if store.completeCalls != 1 {
		t.Fatalf("Complete calls = %d, want 1", store.completeCalls)
	}
	if store.releaseCalls != 0 {
		t.Fatalf("Release calls = %d, want 0", store.releaseCalls)
	}
	if len(messageQueue.acknowledged) != 1 ||
		messageQueue.acknowledged[0] != message.ID {
		t.Fatalf("acknowledged = %v, want [%s]", messageQueue.acknowledged, message.ID)
	}
}

func TestWorkerIdempotencyReleasesLeaseOnProcessingError(t *testing.T) {
	message := queue.Message{ID: "14-0", EventID: 14}
	processingErr := errors.New("processing failed")
	messageQueue := &mockMessageQueue{}
	processor := &mockEventProcessor{err: processingErr}
	store := &mockIdempotencyStore{
		token:    "test-token",
		acquired: true,
	}

	w := newIdempotentTestWorker(messageQueue, processor, store)
	w.processMessages(context.Background(), []queue.Message{message})

	if store.releaseCalls != 1 {
		t.Fatalf("Release calls = %d, want 1", store.releaseCalls)
	}
	if !errors.Is(store.releasedErr, processingErr) {
		t.Fatalf("released error = %v, want %v", store.releasedErr, processingErr)
	}
	if store.completeCalls != 0 {
		t.Fatalf("Complete calls = %d, want 0", store.completeCalls)
	}
	if messageQueue.deadLetterCalls != 1 {
		t.Fatalf("dead-letter calls = %d, want 1", messageQueue.deadLetterCalls)
	}
	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("ACK calls = %d, want 0", len(messageQueue.acknowledged))
	}
}

func TestWorkerIdempotencyReleaseErrorPreventsDeadLetter(t *testing.T) {
	message := queue.Message{ID: "15-0", EventID: 15}
	messageQueue := &mockMessageQueue{}
	processor := &mockEventProcessor{err: errors.New("processing failed")}
	store := &mockIdempotencyStore{
		token:      "test-token",
		acquired:   true,
		releaseErr: errors.New("PostgreSQL unavailable"),
	}

	w := newIdempotentTestWorker(messageQueue, processor, store)
	w.processMessages(context.Background(), []queue.Message{message})

	if store.releaseCalls != 1 {
		t.Fatalf("Release calls = %d, want 1", store.releaseCalls)
	}
	if messageQueue.deadLetterCalls != 0 {
		t.Fatalf("dead-letter calls = %d, want 0", messageQueue.deadLetterCalls)
	}
	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("ACK calls = %d, want 0", len(messageQueue.acknowledged))
	}
}

func TestWorkerIdempotencyCompleteErrorPreventsAck(t *testing.T) {
	message := queue.Message{ID: "16-0", EventID: 16}
	messageQueue := &mockMessageQueue{}
	processor := &mockEventProcessor{}
	store := &mockIdempotencyStore{
		token:       "test-token",
		acquired:    true,
		completeErr: errors.New("PostgreSQL unavailable"),
	}

	w := newIdempotentTestWorker(messageQueue, processor, store)
	w.processMessages(context.Background(), []queue.Message{message})

	if processor.calls != 1 {
		t.Fatalf("processor calls = %d, want 1", processor.calls)
	}
	if store.completeCalls != 1 {
		t.Fatalf("Complete calls = %d, want 1", store.completeCalls)
	}
	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("ACK calls = %d, want 0", len(messageQueue.acknowledged))
	}
	if messageQueue.deadLetterCalls != 0 {
		t.Fatalf("dead-letter calls = %d, want 0", messageQueue.deadLetterCalls)
	}
}

func TestWorkerIdempotencyAckFailureDoesNotRepeatProcessing(t *testing.T) {
	message := queue.Message{ID: "17-0", EventID: 17}
	messageQueue := &mockMessageQueue{
		ackErr: errors.New("Redis unavailable"),
	}
	processor := &mockEventProcessor{}
	store := &mockIdempotencyStore{
		token:    "test-token",
		acquired: true,
	}

	w := newIdempotentTestWorker(messageQueue, processor, store)

	w.processMessages(context.Background(), []queue.Message{message})

	if processor.calls != 1 {
		t.Fatalf("processor calls after first delivery = %d, want 1", processor.calls)
	}
	if store.completeCalls != 1 {
		t.Fatalf("Complete calls = %d, want 1", store.completeCalls)
	}
	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("ACK calls after Redis error = %d, want 0", len(messageQueue.acknowledged))
	}

	// Simulate the state persisted by PostgreSQL after Complete.
	store.acquired = false
	store.completed = true
	messageQueue.ackErr = nil

	w.processMessages(context.Background(), []queue.Message{message})

	if processor.calls != 1 {
		t.Fatalf("processor calls after redelivery = %d, want 1", processor.calls)
	}
	if store.acquireCalls != 2 {
		t.Fatalf("Acquire calls = %d, want 2", store.acquireCalls)
	}
	if store.completeCalls != 1 {
		t.Fatalf("Complete calls after redelivery = %d, want 1", store.completeCalls)
	}
	if len(messageQueue.acknowledged) != 1 ||
		messageQueue.acknowledged[0] != message.ID {
		t.Fatalf("acknowledged = %v, want [%s]", messageQueue.acknowledged, message.ID)
	}
}

type heartbeatTestProcessor struct {
	started  chan struct{}
	release  chan struct{}
	canceled chan struct{}
}

func (p *heartbeatTestProcessor) Process(
	ctx context.Context,
	message queue.Message,
) error {
	close(p.started)

	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		close(p.canceled)
		return ctx.Err()
	}
}

func TestWorkerRenewsLeaseDuringProcessing(t *testing.T) {
	message := queue.Message{ID: "20-0", EventID: 20}
	messageQueue := &mockMessageQueue{}
	processor := &heartbeatTestProcessor{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	store := &mockIdempotencyStore{
		token:       "test-token",
		acquired:    true,
		renewSignal: make(chan struct{}, 1),
	}

	w := NewWorker(messageQueue, processor, "tasks:events", "task-workers", "worker-1")
	w.SetIdempotencyStore(store)
	w.SetLeaseRenewInterval(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		w.processMessages(context.Background(), []queue.Message{message})
		close(done)
	}()

	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("processor did not start")
	}

	select {
	case <-store.renewSignal:
	case <-time.After(time.Second):
		t.Fatal("worker did not renew the idempotency lease")
	}

	close(processor.release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not finish processing")
	}

	if store.renewCalls < 1 {
		t.Fatalf("Renew calls = %d, want at least 1", store.renewCalls)
	}
	if store.completeCalls != 1 {
		t.Fatalf("Complete calls = %d, want 1", store.completeCalls)
	}
	if len(messageQueue.acknowledged) != 1 || messageQueue.acknowledged[0] != message.ID {
		t.Fatalf("acknowledged = %v, want [%s]", messageQueue.acknowledged, message.ID)
	}
}

func TestWorkerLeaseRenewalFailurePreventsAck(t *testing.T) {
	message := queue.Message{ID: "21-0", EventID: 21}
	messageQueue := &mockMessageQueue{}
	processor := &heartbeatTestProcessor{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	store := &mockIdempotencyStore{
		token:       "test-token",
		acquired:    true,
		renewErr:    errors.New("lease lost"),
		renewSignal: make(chan struct{}, 1),
	}

	w := NewWorker(messageQueue, processor, "tasks:events", "task-workers", "worker-1")
	w.SetIdempotencyStore(store)
	w.SetLeaseRenewInterval(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		w.processMessages(context.Background(), []queue.Message{message})
		close(done)
	}()

	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("processor did not start")
	}

	select {
	case <-processor.canceled:
	case <-time.After(time.Second):
		t.Fatal("processing context was not canceled after lease renewal failed")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not finish after lease renewal failure")
	}

	if store.renewCalls < 1 {
		t.Fatalf("Renew calls = %d, want at least 1", store.renewCalls)
	}
	if store.completeCalls != 0 {
		t.Fatalf("Complete calls = %d, want 0", store.completeCalls)
	}
	if store.releaseCalls != 0 {
		t.Fatalf("Release calls = %d, want 0", store.releaseCalls)
	}
	if messageQueue.deadLetterCalls != 0 {
		t.Fatalf("dead-letter calls = %d, want 0", messageQueue.deadLetterCalls)
	}
	if len(messageQueue.acknowledged) != 0 {
		t.Fatalf("ACK calls = %d, want 0", len(messageQueue.acknowledged))
	}
}
