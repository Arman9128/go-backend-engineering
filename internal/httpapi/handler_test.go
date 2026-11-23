package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/task"
)

type mockTaskService struct {
	foundTask         task.Task
	err               error
	calls             int
	requestedID       int64
	updateStatusCalls int
	updatedTaskID     int64
	updatedStatus     string
}

func (m *mockTaskService) GetTaskByID(id int64) (task.Task, error) {
	m.calls++
	m.requestedID = id
	return m.foundTask, m.err
}

func (m *mockTaskService) SubmitTaskWithTags(name string, tags []string) (*task.Task, error) {
	m.calls++
	if m.err != nil {
		return nil, m.err
	}

	createdTask := task.Task{
		ID:        126,
		Name:      name,
		Status:    task.StatusPending,
		CreatedAt: time.Now().UTC(),
		Tags:      append([]string(nil), tags...),
	}

	return &createdTask, nil
}

func (m *mockTaskService) UpdateTaskStatus(id int64, status string) error {
	m.updateStatusCalls++
	m.updatedTaskID = id
	m.updatedStatus = status
	return m.err
}

type mockHealthChecker struct {
	err   error
	calls int
}

func (m *mockHealthChecker) PingContext(ctx context.Context) error {
	m.calls++
	return m.err
}

func TestGetTaskByID(t *testing.T) {
	expectedTask := task.Task{
		ID:        125,
		Name:      "send-email",
		Status:    task.StatusPending,
		CreatedAt: time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC),
		Tags:      []string{"email", "notification"},
	}

	service := &mockTaskService{foundTask: expectedTask}
	handler := NewHandler(service, &mockHealthChecker{})

	request := httptest.NewRequest(http.MethodGet, "/tasks/125", nil)
	recorder := httptest.NewRecorder()

	handler.Routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", recorder.Code)
	}

	var response taskResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if response.ID != expectedTask.ID {
		t.Errorf("expected ID %d, got %d", expectedTask.ID, response.ID)
	}

	if response.Name != expectedTask.Name {
		t.Errorf("expected name %q, got %q", expectedTask.Name, response.Name)
	}

	if len(response.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(response.Tags))
	}

	if service.calls != 1 || service.requestedID != 125 {
		t.Errorf("unexpected service call: calls=%d, ID=%d", service.calls, service.requestedID)
	}
}

func TestGetTaskByIDInvalidID(t *testing.T) {
	service := &mockTaskService{}
	handler := NewHandler(service, &mockHealthChecker{})

	tests := []string{
		"/tasks/abc",
		"/tasks/0",
		"/tasks/-1",
	}

	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			recorder := httptest.NewRecorder()

			handler.Routes().ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Errorf("expected status 400, got %d", recorder.Code)
			}

			if service.calls != 0 {
				t.Errorf("service should not be called for invalid ID")
			}
		})
	}
}

func TestGetTaskByIDNotFound(t *testing.T) {
	service := &mockTaskService{
		err: errors.Join(errors.New("storage lookup failed"), task.ErrNotFound),
	}
	handler := NewHandler(service, &mockHealthChecker{})

	request := httptest.NewRequest(http.MethodGet, "/tasks/999", nil)
	recorder := httptest.NewRecorder()

	handler.Routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", recorder.Code)
	}
}

func TestGetTaskByIDInternalError(t *testing.T) {
	service := &mockTaskService{
		err: errors.New("database connection failed"),
	}
	handler := NewHandler(service, &mockHealthChecker{})

	request := httptest.NewRequest(http.MethodGet, "/tasks/125", nil)
	recorder := httptest.NewRecorder()

	handler.Routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", recorder.Code)
	}
}

func TestHealth(t *testing.T) {
	db := &mockHealthChecker{}
	handler := NewHandler(&mockTaskService{}, db)

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder := httptest.NewRecorder()

	handler.Routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", recorder.Code)
	}

	if db.calls != 1 {
		t.Errorf("expected 1 database ping, got %d", db.calls)
	}
}

func TestHealthDatabaseUnavailable(t *testing.T) {
	db := &mockHealthChecker{
		err: errors.New("database unavailable"),
	}
	handler := NewHandler(&mockTaskService{}, db)

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder := httptest.NewRecorder()

	handler.Routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", recorder.Code)
	}
}

func TestCreateTask(t *testing.T) {
	service := &mockTaskService{}
	handler := NewHandler(service, &mockHealthChecker{})

	body := `{
		"name": "send-email",
		"tags": ["email", "notification"]
	}`

	request := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	handler.Routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var response taskResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if response.ID != 126 {
		t.Errorf("expected ID 126, got %d", response.ID)
	}

	if response.Name != "send-email" {
		t.Errorf("expected name send-email, got %q", response.Name)
	}

	if response.Status != task.StatusPending {
		t.Errorf("expected status pending, got %q", response.Status)
	}

	if len(response.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(response.Tags))
	}

	if service.calls != 1 {
		t.Errorf("expected 1 service call, got %d", service.calls)
	}
}

func TestCreateTaskInvalidRequest(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "invalid JSON",
			body: `{"name":`,
		},
		{
			name: "empty name",
			body: `{"name":"   "}`,
		},
		{
			name: "unknown field",
			body: `{"name":"send-email","unknown":"value"}`,
		},
		{
			name: "multiple JSON values",
			body: `{"name":"send-email"} {"name":"another-task"}`,
		},
		{
			name: "JSON array instead of object",
			body: `["send-email"]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &mockTaskService{}
			handler := NewHandler(service, &mockHealthChecker{})

			request := httptest.NewRequest(
				http.MethodPost,
				"/tasks",
				strings.NewReader(test.body),
			)
			recorder := httptest.NewRecorder()

			handler.Routes().ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Errorf("expected status 400, got %d", recorder.Code)
			}

			if service.calls != 0 {
				t.Errorf("service should not be called, got %d calls", service.calls)
			}
		})
	}
}

func TestCreateTaskServiceError(t *testing.T) {
	service := &mockTaskService{
		err: errors.New("database connection failed"),
	}
	handler := NewHandler(service, &mockHealthChecker{})

	request := httptest.NewRequest(
		http.MethodPost,
		"/tasks",
		strings.NewReader(`{"name":"send-email"}`),
	)
	recorder := httptest.NewRecorder()

	handler.Routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", recorder.Code)
	}

	if service.calls != 1 {
		t.Errorf("expected 1 service call, got %d", service.calls)
	}
}

func TestCreateTaskBodyTooLarge(t *testing.T) {
	service := &mockTaskService{}
	handler := NewHandler(service, &mockHealthChecker{})

	body := `{"name":"` + strings.Repeat("a", 1<<20) + `"}`

	request := httptest.NewRequest(
		http.MethodPost,
		"/tasks",
		strings.NewReader(body),
	)
	recorder := httptest.NewRecorder()

	handler.Routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", recorder.Code)
	}

	if service.calls != 0 {
		t.Errorf("service should not be called, got %d calls", service.calls)
	}
}

func TestUpdateTaskStatus(t *testing.T) {
	service := &mockTaskService{}
	handler := NewHandler(service, &mockHealthChecker{})

	request := httptest.NewRequest(
		http.MethodPatch,
		"/tasks/5/status",
		strings.NewReader(`{"status":"processing"}`),
	)
	request.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	handler.Routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d: %s", recorder.Code, recorder.Body.String())
	}

	if recorder.Body.Len() != 0 {
		t.Errorf("expected empty response body, got %q", recorder.Body.String())
	}

	if service.updateStatusCalls != 1 {
		t.Fatalf("expected 1 update call, got %d", service.updateStatusCalls)
	}

	if service.updatedTaskID != 5 {
		t.Errorf("expected task ID 5, got %d", service.updatedTaskID)
	}

	if service.updatedStatus != task.StatusProcessing {
		t.Errorf("expected status %q, got %q", task.StatusProcessing, service.updatedStatus)
	}
}

func TestUpdateTaskStatusInvalidRequest(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "invalid ID", path: "/tasks/abc/status", body: `{"status":"processing"}`},
		{name: "zero ID", path: "/tasks/0/status", body: `{"status":"processing"}`},
		{name: "invalid JSON", path: "/tasks/5/status", body: `{"status":`},
		{name: "missing status", path: "/tasks/5/status", body: `{}`},
		{name: "empty status", path: "/tasks/5/status", body: `{"status":"  "}`},
		{name: "unknown field", path: "/tasks/5/status", body: `{"status":"processing","extra":true}`},
		{name: "multiple JSON values", path: "/tasks/5/status", body: `{"status":"processing"} {}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &mockTaskService{}
			handler := NewHandler(service, &mockHealthChecker{})

			request := httptest.NewRequest(
				http.MethodPatch,
				test.path,
				strings.NewReader(test.body),
			)
			recorder := httptest.NewRecorder()

			handler.Routes().ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Errorf("expected status 400, got %d", recorder.Code)
			}

			if service.updateStatusCalls != 0 {
				t.Errorf("service should not be called, got %d calls", service.updateStatusCalls)
			}
		})
	}
}

func TestUpdateTaskStatusServiceErrors(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedStatus int
	}{
		{
			name:           "task not found",
			err:            task.ErrNotFound,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "invalid status",
			err:            task.ErrInvalidStatus,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "internal error",
			err:            errors.New("database unavailable"),
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &mockTaskService{err: test.err}
			handler := NewHandler(service, &mockHealthChecker{})

			request := httptest.NewRequest(
				http.MethodPatch,
				"/tasks/5/status",
				strings.NewReader(`{"status":"processing"}`),
			)
			recorder := httptest.NewRecorder()

			handler.Routes().ServeHTTP(recorder, request)

			if recorder.Code != test.expectedStatus {
				t.Errorf("expected status %d, got %d", test.expectedStatus, recorder.Code)
			}

			if service.updateStatusCalls != 1 {
				t.Errorf("expected 1 update call, got %d", service.updateStatusCalls)
			}
		})
	}
}
