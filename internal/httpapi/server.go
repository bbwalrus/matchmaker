package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"matchmaker/internal/matchmaking"
	"matchmaker/internal/model"
	"matchmaker/internal/realtime"
)

type queueStore interface {
	Join(context.Context, model.Ticket) error
	Metrics(context.Context, string) (model.QueueMetrics, error)
	ActiveLobbies(context.Context, string) ([]model.Lobby, error)
}

type Server struct {
	store                 queueStore
	hub                   *realtime.Hub
	allowedFrontendOrigin string
}

func NewServer(store queueStore, hub *realtime.Hub, allowedFrontendOrigin ...string) *Server {
	server := &Server{store: store, hub: hub}
	if len(allowedFrontendOrigin) > 0 {
		server.allowedFrontendOrigin = strings.TrimRight(allowedFrontendOrigin[0], "/")
	}
	return server
}

func (s *Server) Handler() http.Handler {
	router := chi.NewRouter()
	router.Use(s.cors)
	router.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	router.Post("/api/queue/join", s.joinQueue)
	router.Post("/api/queue/simulate", s.simulate)
	router.Get("/api/metrics", s.queueMetrics)
	router.Get("/api/lobbies", s.activeLobbies)
	router.Get("/ws", s.hub.ServeWSWithOrigin(s.allowedFrontendOrigin))
	return router
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && s.allowedFrontendOrigin != "" {
			if origin != s.allowedFrontendOrigin && !sameOrigin(origin, r.Host) {
				http.Error(w, "origin not allowed", http.StatusForbidden)
				return
			}
			if origin == s.allowedFrontendOrigin {
				w.Header().Set("Access-Control-Allow-Origin", s.allowedFrontendOrigin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
				w.Header().Add("Vary", "Origin")
			}
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func sameOrigin(origin, host string) bool {
	parsed, err := url.Parse(origin)
	return err == nil && parsed.Host == host && (parsed.Scheme == "http" || parsed.Scheme == "https")
}

func (s *Server) queueMetrics(w http.ResponseWriter, r *http.Request) {
	region := r.URL.Query().Get("region")
	if !validRegion(region) {
		http.Error(w, "a supported region query parameter is required", http.StatusBadRequest)
		return
	}
	metrics, err := s.store.Metrics(r.Context(), region)
	if err != nil {
		http.Error(w, fmt.Sprintf("queue metrics failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		http.Error(w, "failed to encode queue metrics", http.StatusInternalServerError)
	}
}

func (s *Server) activeLobbies(w http.ResponseWriter, r *http.Request) {
	region := r.URL.Query().Get("region")
	if !validRegion(region) {
		http.Error(w, "a supported region query parameter is required", http.StatusBadRequest)
		return
	}
	lobbies, err := s.store.ActiveLobbies(r.Context(), region)
	if err != nil {
		http.Error(w, fmt.Sprintf("active lobby lookup failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(lobbies); err != nil {
		http.Error(w, "failed to encode active lobbies", http.StatusInternalServerError)
	}
}

type joinRequest struct {
	PlayerID string `json:"player_id"`
	MMR      int    `json:"mmr"`
	Region   string `json:"region"`
}

func (s *Server) joinQueue(w http.ResponseWriter, r *http.Request) {
	var req joinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.PlayerID) == "" || !validRegion(req.Region) || req.MMR < 0 || req.MMR > 5000 {
		http.Error(w, "player_id, supported region, and MMR between 0 and 5000 are required", http.StatusBadRequest)
		return
	}
	ticket := model.Ticket{
		PlayerID: req.PlayerID,
		MMR:      req.MMR,
		Region:   req.Region,
		JoinedAt: time.Now().UTC(),
	}
	if err := s.store.Join(r.Context(), ticket); err != nil {
		if errors.Is(err, matchmaking.ErrAlreadyQueued) {
			http.Error(w, "player already has a queued ticket", http.StatusConflict)
			return
		}
		http.Error(w, fmt.Sprintf("queue join failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ticket)
}

type simulateRequest struct {
	Count  int    `json:"count"`
	Region string `json:"region"`
	Mean   int    `json:"mean_mmr"`
	StdDev int    `json:"stddev_mmr"`
}

func (s *Server) simulate(w http.ResponseWriter, r *http.Request) {
	var req simulateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Count < 1 || req.Count > 500 || !validRegion(req.Region) || req.Mean < 0 || req.Mean > 5000 || req.StdDev < 1 || req.StdDev > 1000 {
		http.Error(w, "count (1-500), supported region, mean_mmr (0-5000), and stddev_mmr (1-1000) are required", http.StatusBadRequest)
		return
	}
	for i := 0; i < req.Count; i++ {
		mmr := int(rand.NormFloat64()*float64(req.StdDev) + float64(req.Mean))
		if mmr < 0 {
			mmr = 0
		} else if mmr > 5000 {
			mmr = 5000
		}
		ticket := model.Ticket{
			PlayerID: fmt.Sprintf("bot-%d-%d", time.Now().UnixNano(), i),
			MMR:      mmr,
			Region:   req.Region,
			JoinedAt: time.Now().UTC(),
			IsBot:    true,
		}
		if err := s.store.Join(r.Context(), ticket); err != nil {
			http.Error(w, fmt.Sprintf("bot ingestion stopped after %d tickets: %v", i, err), http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"accepted": req.Count})
}

func validRegion(region string) bool {
	for _, supported := range matchmaking.Regions {
		if region == supported {
			return true
		}
	}
	return false
}
