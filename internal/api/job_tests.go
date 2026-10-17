package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/WW-Keith/gotaskq/internal/broker"
	"github.com/WW-Keith/gotaskq/internal/model"
	"github.com/WW-Keith/gotaskq/internal/observe"
	"github.com/WW-Keith/gotaskq/internal/store"
)

// --- fake store for unit tests — no real database needed ---

type fakeStore struct {
	jobs   map[string]*model.Job
	nextID int
}

// add this below fakeStore in jobs_test.go

type fakeBroker struct{}

func (f *fakeStore) ScheduleRetry(_ context.Context, _, _ string, _ time.Time) error {
	return nil
}
func (f *fakeStore) MarkFailed(_ context.Context, _, _ string) error {
	return nil
}
func (f *fakeStore) MarkPublished(_ context.Context, _ string) error { return nil }
func (f *fakeStore) CreateSchedule(_ context.Context, s *model.Schedule) error {
	s.ID = "fake-schedule"
	return nil
}
func (f *fakeStore) DeleteSchedule(_ context.Context, _ string) error { return nil }
func (f *fakeStore) MaterializeDueSchedules(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

func (f *fakeBroker) Publish(_ context.Context, _, _ string) error { return nil }
func (f *fakeBroker) Subscribe(_ context.Context, _, _ string) (*broker.Message, error) {
	return nil, nil
}
func (f *fakeBroker) Ack(_ context.Context, _, _ string) error      { return nil }
func (f *fakeBroker) EnsureGroup(_ context.Context, _ string) error { return nil }

// update newTestServer to pass the fake broker
//func newTestServer() *Server {
//	return NewServer(newFakeStore(), &fakeBroker{})
//}

func newFakeStore() *fakeStore {
	return &fakeStore{jobs: make(map[string]*model.Job)}
}

func (f *fakeStore) Enqueue(_ context.Context, job *model.Job) error {
	if err := job.Valid(); err != nil {
		return err
	}
	f.nextID++
	job.ID = fmt.Sprintf("fake-%d", f.nextID)
	job.Status = model.StatusPending
	if job.Queue == "" {
		job.Queue = "default"
	}
	f.jobs[job.ID] = job
	return nil
}

func (f *fakeStore) GetByID(_ context.Context, id string) (*model.Job, error) {
	job, ok := f.jobs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return job, nil
}

func (f *fakeStore) Cancel(_ context.Context, id string) error {
	job, ok := f.jobs[id]
	if !ok {
		return store.ErrNotFound
	}
	//if job.Status != model.StatusPending {
	//	return store.ErrCannotCancel
	//}
	job.Status = model.StatusCancelled
	return nil
}

// Remaining Store methods — not used in API tests yet
func (f *fakeStore) Dequeue(_ context.Context, _ string) (*model.Job, error) { return nil, nil }
func (f *fakeStore) Claim(_ context.Context, _ string) (*model.Job, error)   { return nil, nil }
func (f *fakeStore) Ack(_ context.Context, _ string) error                   { return nil }
func (f *fakeStore) Nack(_ context.Context, _, _ string) error               { return nil }
func (f *fakeStore) Ping(_ context.Context) error                            { return nil }
func (f *fakeStore) List(_ context.Context, _ model.Status, _, _ int) ([]*model.Job, error) {
	return nil, nil
}

// --- helpers ---

func newTestServer() *Server {
	return NewServer(newFakeStore(), &fakeBroker{}, observe.New())
}

func doRequest(t *testing.T, srv *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode request body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

// --- tests ---

func TestHandleEnqueue_Success(t *testing.T) {
	srv := newTestServer()

	rr := doRequest(t, srv, "POST", "/jobs", map[string]any{
		"type":    "send_email",
		"payload": map[string]string{"to": "keith@example.com"},
	})

	if rr.Code != http.StatusCreated {
		t.Errorf("status: got %d, want %d — body: %s",
			rr.Code, http.StatusCreated, rr.Body.String())
	}

	var job model.Job
	if err := json.NewDecoder(rr.Body).Decode(&job); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if job.ID == "" {
		t.Error("expected ID in response, got empty")
	}
	if job.Status != model.StatusPending {
		t.Errorf("status: got %q, want %q", job.Status, model.StatusPending)
	}
	if job.Type != "send_email" {
		t.Errorf("type: got %q, want %q", job.Type, "send_email")
	}

	t.Logf("created job id=%s", job.ID)
}

func TestHandleEnqueue_InvalidJSON(t *testing.T) {
	srv := newTestServer()

	req := httptest.NewRequest("POST", "/jobs",
		bytes.NewBufferString(`{not valid json`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestHandleEnqueue_MissingType(t *testing.T) {
	srv := newTestServer()

	rr := doRequest(t, srv, "POST", "/jobs", map[string]any{
		"payload": map[string]string{"to": "keith@example.com"},
		// "type" deliberately omitted
	})

	if rr.Code != http.StatusUnprocessableEntity {
		t.Errorf("status: got %d, want %d — body: %s",
			rr.Code, http.StatusUnprocessableEntity, rr.Body.String())
	}
}

func TestHandleGetJob_Found(t *testing.T) {
	srv := newTestServer()

	// Create a job first
	rr := doRequest(t, srv, "POST", "/jobs", map[string]any{
		"type":    "resize_image",
		"payload": map[string]string{"url": "https://example.com/img.png"},
	})
	if rr.Code != http.StatusCreated {
		t.Fatalf("setup enqueue failed: %s", rr.Body.String())
	}

	var created model.Job
	json.NewDecoder(rr.Body).Decode(&created)

	// Now fetch it
	rr2 := doRequest(t, srv, "GET", "/jobs/"+created.ID, nil)
	if rr2.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d — body: %s",
			rr2.Code, http.StatusOK, rr2.Body.String())
	}

	var fetched model.Job
	json.NewDecoder(rr2.Body).Decode(&fetched)
	if fetched.ID != created.ID {
		t.Errorf("id: got %q, want %q", fetched.ID, created.ID)
	}
}

func TestHandleGetJob_NotFound(t *testing.T) {
	srv := newTestServer()

	rr := doRequest(t, srv, "GET", "/jobs/does-not-exist", nil)
	if rr.Code != http.StatusNotFound {
		t.Errorf("status: got %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestHandleCancel_Success(t *testing.T) {
	srv := newTestServer()

	// Enqueue a job
	rr := doRequest(t, srv, "POST", "/jobs", map[string]any{
		"type":    "gen_report",
		"payload": map[string]string{"month": "jan"},
	})
	var created model.Job
	json.NewDecoder(rr.Body).Decode(&created)

	// Cancel it
	rr2 := doRequest(t, srv, "DELETE", "/jobs/"+created.ID, nil)
	if rr2.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d — body: %s",
			rr2.Code, http.StatusOK, rr2.Body.String())
	}
}

func TestHandleCancel_NotFound(t *testing.T) {
	srv := newTestServer()

	rr := doRequest(t, srv, "DELETE", "/jobs/ghost-job", nil)
	if rr.Code != http.StatusNotFound {
		t.Errorf("status: got %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestHandleHealth(t *testing.T) {
	srv := newTestServer()

	rr := doRequest(t, srv, "GET", "/health", nil)
	if rr.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", rr.Code, http.StatusOK)
	}
}
