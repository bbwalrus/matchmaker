package model

import "time"

type Ticket struct {
	PlayerID string    `json:"player_id"`
	MMR      int       `json:"mmr"`
	Region   string    `json:"region"`
	JoinedAt time.Time `json:"joined_at"`
	IsBot    bool      `json:"is_bot,omitempty"`
}

type Match struct {
	ID             string    `json:"id"`
	Region         string    `json:"region"`
	CreatedAt      time.Time `json:"created_at"`
	LobbyExpiresAt time.Time `json:"lobby_expires_at"`
	Players        []Ticket  `json:"players"`
	TeamOne        []Ticket  `json:"team_one"`
	TeamTwo        []Ticket  `json:"team_two"`
}

type Lobby struct {
	ID        string    `json:"id"`
	MatchID   string    `json:"match_id"`
	Region    string    `json:"region"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Players   []Ticket  `json:"players"`
	TeamOne   []Ticket  `json:"team_one"`
	TeamTwo   []Ticket  `json:"team_two"`
}

type MMRBucket struct {
	Min   int `json:"min"`
	Max   int `json:"max"`
	Count int `json:"count"`
}

type QueueMetrics struct {
	Region           string      `json:"region"`
	QueueSize        int64       `json:"queue_size"`
	MMRDistribution  []MMRBucket `json:"mmr_distribution"`
	AverageWaitMS    int64       `json:"average_wait_ms"`
	IngestionPerSec  float64     `json:"ingestion_per_sec,omitempty"`
	ActiveWebSockets int         `json:"active_websockets,omitempty"`
	MatchLatencyMS   int64       `json:"match_latency_ms,omitempty"`
	UpdatedAt        time.Time   `json:"updated_at"`
}
