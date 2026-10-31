package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"matchmaker/internal/model"
	"matchmaker/internal/realtime"
)

type fakeQueueStore struct {
	metrics model.QueueMetrics
	lobbies []model.Lobby
	err     error
}

func (s fakeQueueStore) Join(context.Context, model.Ticket) error { return s.err }

func (s fakeQueueStore) Metrics(context.Context, string) (model.QueueMetrics, error) {
	return s.metrics, s.err
}

func (s fakeQueueStore) ActiveLobbies(context.Context, string) ([]model.Lobby, error) {
	return s.lobbies, s.err
}

func TestMetricsEndpointReturnsRegionMetrics(t *testing.T) {
	want := model.QueueMetrics{
		Region:          "na-west",
		QueueSize:       2,
		MMRDistribution: []model.MMRBucket{{Min: 0, Max: 499, Count: 2}},
		UpdatedAt:       time.Now().UTC(),
	}
	server := NewServer(fakeQueueStore{metrics: want}, realtime.NewHub())
	request := httptest.NewRequest(http.MethodGet, "/api/metrics?region=na-west", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var got model.QueueMetrics
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Region != want.Region || got.QueueSize != want.QueueSize {
		t.Errorf("metrics = %+v, want region %s and queue size %d", got, want.Region, want.QueueSize)
	}
}

func TestMetricsEndpointRequiresSupportedRegion(t *testing.T) {
	server := NewServer(fakeQueueStore{}, realtime.NewHub())
	request := httptest.NewRequest(http.MethodGet, "/api/metrics?region=unknown", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestActiveLobbiesEndpointSurfacesStoreErrors(t *testing.T) {
	server := NewServer(fakeQueueStore{err: errors.New("Redis unavailable")}, realtime.NewHub())
	request := httptest.NewRequest(http.MethodGet, "/api/lobbies?region=na-west", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
}

func TestActiveLobbiesEndpointReturnsLobbies(t *testing.T) {
	want := []model.Lobby{{ID: "lobby-1", MatchID: "match-1", Region: "na-west"}}
	server := NewServer(fakeQueueStore{lobbies: want}, realtime.NewHub())
	request := httptest.NewRequest(http.MethodGet, "/api/lobbies?region=na-west", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var got []model.Lobby
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got) != 1 || got[0].ID != want[0].ID {
		t.Errorf("lobbies = %+v, want %+v", got, want)
	}
}

func TestConfiguredFrontendOriginCanCallAPI(t *testing.T) {
	server := NewServer(
		fakeQueueStore{metrics: model.QueueMetrics{Region: "na-west"}},
		realtime.NewHub(),
		"https://matchmaker.vercel.app",
	)
	request := httptest.NewRequest(http.MethodGet, "/api/metrics?region=na-west", nil)
	request.Header.Set("Origin", "https://matchmaker.vercel.app")
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Access-Control-Allow-Origin"); got != "https://matchmaker.vercel.app" {
		t.Errorf("allow-origin = %q, want configured frontend origin", got)
	}
}

func TestConfiguredFrontendOriginRejectsOtherOrigins(t *testing.T) {
	server := NewServer(fakeQueueStore{}, realtime.NewHub(), "https://matchmaker.vercel.app")
	request := httptest.NewRequest(http.MethodGet, "/api/metrics?region=na-west", nil)
	request.Header.Set("Origin", "https://unexpected.example")
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
}
