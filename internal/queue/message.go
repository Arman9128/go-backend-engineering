package queue

type Message struct {
	ID        string
	EventID   int64
	EventType string
	Payload   []byte
}
