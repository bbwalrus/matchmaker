package matchmaking

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"matchmaker/internal/model"
)

func newTestStore(t *testing.T) (*miniredis.Miniredis, *RedisStore) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close Redis client: %v", err)
		}
	})
	return server, NewRedisStore(client)
}

func TestMetricsCountsQueueAndMMRBuckets(t *testing.T) {
	_, store := newTestStore(t)
	ctx := context.Background()
	joinedAt := time.Now().Add(-2 * time.Second)
	for i, mmr := range []int{1200, 1499, 1500, 5000} {
		ticket := model.Ticket{
			PlayerID: fmt.Sprintf("player-%d", i),
			MMR:      mmr,
			Region:   "na-west",
			JoinedAt: joinedAt,
		}
		if err := store.Join(ctx, ticket); err != nil {
			t.Fatalf("join ticket %s: %v", ticket.PlayerID, err)
		}
	}

	metrics, err := store.Metrics(ctx, "na-west")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	if metrics.QueueSize != 4 {
		t.Errorf("queue size = %d, want 4", metrics.QueueSize)
	}
	if len(metrics.MMRDistribution) != 11 {
		t.Fatalf("got %d MMR buckets, want 11", len(metrics.MMRDistribution))
	}
	if got := metrics.MMRDistribution[2].Count; got != 2 {
		t.Errorf("bucket 1000-1499 count = %d, want 2", got)
	}
	if got := metrics.MMRDistribution[3].Count; got != 1 {
		t.Errorf("bucket 1500-1999 count = %d, want 1", got)
	}
	if got := metrics.MMRDistribution[10].Count; got != 1 {
		t.Errorf("bucket 5000 count = %d, want 1", got)
	}
	if metrics.AverageWaitMS < 1500 {
		t.Errorf("average wait = %d ms, want at least 1500 ms", metrics.AverageWaitMS)
	}
	if metrics.UpdatedAt.IsZero() {
		t.Error("updated_at is zero")
	}
}

func TestExpiredBotsAreRemovedAndRealPlayersRemain(t *testing.T) {
	server, store := newTestStore(t)
	ctx := context.Background()
	for _, ticket := range []model.Ticket{
		{PlayerID: "old-bot", MMR: 1200, Region: "na-west", JoinedAt: time.Now().Add(-6 * time.Minute), IsBot: true},
		{PlayerID: "real-player", MMR: 2200, Region: "na-west", JoinedAt: time.Now().Add(-6 * time.Minute)},
	} {
		if err := store.Join(ctx, ticket); err != nil {
			t.Fatalf("join ticket %s: %v", ticket.PlayerID, err)
		}
	}

	server.SetTime(time.Now().Add(DefaultBotQueueTTL + time.Second))
	expired, err := store.ExpireBotTickets(ctx, "na-west")
	if err != nil {
		t.Fatalf("expire bots: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expired %d bot tickets, want 1", expired)
	}

	metrics, err := store.Metrics(ctx, "na-west")
	if err != nil {
		t.Fatalf("get metrics after expiration: %v", err)
	}
	if metrics.QueueSize != 1 {
		t.Errorf("queue size after expiration = %d, want 1", metrics.QueueSize)
	}
	if got := metrics.MMRDistribution[2].Count; got != 0 {
		t.Errorf("bot MMR bucket still contains %d players, want 0", got)
	}
	if got := metrics.MMRDistribution[4].Count; got != 1 {
		t.Errorf("real player's bucket count = %d, want 1", got)
	}
	if exists, err := store.client.Exists(ctx, ticketKey("old-bot")).Result(); err != nil {
		t.Fatalf("check expired bot ticket: %v", err)
	} else if exists != 0 {
		t.Error("expired bot ticket hash still exists")
	}
	if exists, err := store.client.Exists(ctx, ticketKey("real-player")).Result(); err != nil {
		t.Fatalf("check real player ticket: %v", err)
	} else if exists != 1 {
		t.Error("real player ticket was removed")
	}
}

func TestConcurrentWorkersClaimTicketsOnceAndLobbyExpires(t *testing.T) {
	server, store := newTestStore(t)
	store.lobbyTTL = 2 * time.Second
	ctx := context.Background()
	joinedAt := time.Now().Add(-10 * time.Minute)
	for i := 0; i < playersPerMatch; i++ {
		ticket := model.Ticket{
			PlayerID: fmt.Sprintf("player-%d", i),
			MMR:      1200,
			Region:   "na-west",
			JoinedAt: joinedAt,
			IsBot:    true,
		}
		if err := store.Join(ctx, ticket); err != nil {
			t.Fatalf("join ticket %s: %v", ticket.PlayerID, err)
		}
	}

	var wait sync.WaitGroup
	matches := make(chan *model.Match, 2)
	errors := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			match, err := store.MatchOnce(ctx, "na-west")
			if err != nil {
				errors <- err
				return
			}
			matches <- match
		}()
	}
	wait.Wait()
	close(matches)
	close(errors)
	for err := range errors {
		t.Fatalf("matching worker: %v", err)
	}

	matchCount := 0
	claimedPlayers := make(map[string]bool)
	for match := range matches {
		if match == nil {
			continue
		}
		matchCount++
		for _, player := range match.Players {
			if claimedPlayers[player.PlayerID] {
				t.Errorf("player %s was claimed more than once", player.PlayerID)
			}
			claimedPlayers[player.PlayerID] = true
		}
	}
	if matchCount != 1 {
		t.Fatalf("got %d matches, want exactly 1", matchCount)
	}
	if len(claimedPlayers) != playersPerMatch {
		t.Errorf("got %d unique claimed players, want %d", len(claimedPlayers), playersPerMatch)
	}

	active, err := store.ActiveLobbies(ctx, "na-west")
	if err != nil {
		t.Fatalf("read active lobbies: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("got %d active lobbies, want 1", len(active))
	}
	if active[0].ExpiresAt.IsZero() {
		t.Error("lobby expiration is missing")
	}
	if historyLength, err := store.client.XLen(ctx, matchEventsKey).Result(); err != nil {
		t.Fatalf("read match history length: %v", err)
	} else if historyLength != 1 {
		t.Errorf("match history length = %d, want 1", historyLength)
	}

	server.FastForward(3 * time.Second)
	active, err = store.ActiveLobbies(ctx, "na-west")
	if err != nil {
		t.Fatalf("read lobbies after expiry: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("got %d active lobbies after expiry, want 0", len(active))
	}
	if historyLength, err := store.client.XLen(ctx, matchEventsKey).Result(); err != nil {
		t.Fatalf("read match history after expiry: %v", err)
	} else if historyLength != 1 {
		t.Errorf("history was removed at lobby expiry: length = %d, want 1", historyLength)
	}
	if expiryCount, err := store.client.ZCard(ctx, botExpiryIndexKey("na-west")).Result(); err != nil {
		t.Fatalf("read bot expiry index: %v", err)
	} else if expiryCount != 0 {
		t.Errorf("matched bots remain in expiry index: count = %d, want 0", expiryCount)
	}
	for playerID := range claimedPlayers {
		if status, err := store.client.HGet(ctx, ticketKey(playerID), "status").Result(); err != nil {
			t.Errorf("read matched ticket %s after lobby expiry: %v", playerID, err)
		} else if status != "matched" {
			t.Errorf("ticket %s status = %q after lobby expiry, want matched", playerID, status)
		}
	}
}
