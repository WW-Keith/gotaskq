package observe

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerExposesMetrics(t *testing.T) {
	metrics := New()
	metrics.JobsEnqueued.WithLabelValues("default", "email").Inc()

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))

	if response.Code != 200 {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	if !strings.Contains(response.Body.String(), `gotaskq_jobs_enqueued_total{queue="default",type="email"} 1`) {
		t.Fatalf("metrics response missing enqueued counter: %s", response.Body.String())
	}
}
