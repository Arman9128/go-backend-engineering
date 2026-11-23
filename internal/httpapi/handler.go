package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/task"
)

type TaskService interface {
	SubmitTaskWithTags(name string, tags []string) (*task.Task, error)
	GetTaskByID(id int64) (task.Task, error)
	UpdateTaskStatus(id int64, status string) error
}

type HealthChecker interface {
	PingContext(ctx context.Context) error
}

type Handler struct {
	taskService TaskService
	db          HealthChecker
}

func NewHandler(taskService TaskService, db HealthChecker) *Handler {
	return &Handler{
		taskService: taskService,
		db:          db,
	}
}

type createTaskRequest struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

type updateTaskStatusRequest struct {
	Status string `json:"status"`
}

type taskResponse struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Tags      []string  `json:"tags"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type healthResponse struct {
	Status   string `json:"status"`
	Database string `json:"database"`
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /tasks", h.createTask)
	mux.HandleFunc("GET /tasks/{id}", h.getTaskByID)
	mux.HandleFunc("PATCH /tasks/{id}/status", h.updateTaskStatus)

	return mux
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if err := h.db.PingContext(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, healthResponse{
			Status:   "unavailable",
			Database: "unavailable",
		})
		return
	}

	writeJSON(w, http.StatusOK, healthResponse{
		Status:   "ok",
		Database: "ok",
	})
}

func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var request createTaskRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "invalid JSON request body",
		})
		return
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "request body must contain a single JSON object",
		})
		return
	}

	if strings.TrimSpace(request.Name) == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "task name is required",
		})
		return
	}

	createdTask, err := h.taskService.SubmitTaskWithTags(request.Name, request.Tags)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error: "failed to create task",
		})
		return
	}

	writeJSON(w, http.StatusCreated, newTaskResponse(createdTask))
}

func (h *Handler) getTaskByID(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "invalid task ID",
		})
		return
	}

	foundTask, err := h.taskService.GetTaskByID(id)
	if err != nil {
		if errors.Is(err, task.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, errorResponse{
				Error: "task not found",
			})
			return
		}

		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error: "internal server error",
		})
		return
	}

	writeJSON(w, http.StatusOK, newTaskResponse(&foundTask))
}

func (h *Handler) updateTaskStatus(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "invalid task ID",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var request updateTaskStatusRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "invalid JSON request body",
		})
		return
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "request body must contain a single JSON object",
		})
		return
	}

	if strings.TrimSpace(request.Status) == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "status is required",
		})
		return
	}

	err = h.taskService.UpdateTaskStatus(id, request.Status)
	if err != nil {
		switch {
		case errors.Is(err, task.ErrNotFound):
			writeJSON(w, http.StatusNotFound, errorResponse{
				Error: "task not found",
			})
		case errors.Is(err, task.ErrInvalidStatus):
			writeJSON(w, http.StatusBadRequest, errorResponse{
				Error: "invalid task status",
			})
		default:
			writeJSON(w, http.StatusInternalServerError, errorResponse{
				Error: "failed to update task status",
			})
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func newTaskResponse(taskData *task.Task) taskResponse {
	tags := taskData.Tags
	if tags == nil {
		tags = []string{}
	}

	return taskResponse{
		ID:        taskData.ID,
		Name:      taskData.Name,
		Status:    taskData.Status,
		CreatedAt: taskData.CreatedAt,
		Tags:      tags,
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	body, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}
