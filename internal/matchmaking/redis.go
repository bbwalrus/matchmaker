package matchmaking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"matchmaker/internal/model"
)

var ErrAlreadyQueued = errors.New("player already has a queued ticket")

const (
	playersPerMatch = 10
	baseMMRGap      = 50
	mmrPerWaitSec   = 10
	mmrBucketWidth  = 500
	mmrMaximum      = 5000
	expiryBatchSize = 500
)

const DefaultLobbyTTL = 5 * time.Minute
const DefaultBotQueueTTL = 5 * time.Minute

var Regions = []string{"na-west", "na-east", "eu-west"}

type RedisStore struct {
	client   *redis.Client
	join     *redis.Script
	claim    *redis.Script
	expire   *redis.Script
	lobbyTTL time.Duration
}

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{
		client:   client,
		join:     redis.NewScript(joinTicketLua),
		claim:    redis.NewScript(claimMatchLua),
		expire:   redis.NewScript(expireBotsLua),
		lobbyTTL: DefaultLobbyTTL,
	}
}

func (s *RedisStore) Join(ctx context.Context, ticket model.Ticket) error {
	joinedAt := ticket.JoinedAt.UnixMilli()
	isBot := "0"
	if ticket.IsBot {
		isBot = "1"
	}
	joined, err := s.join.Run(
		ctx,
		s.client,
		[]string{queueKey(ticket.Region), ticketKey(ticket.PlayerID), botExpiryIndexKey(ticket.Region)},
		ticket.PlayerID,
		strconv.Itoa(ticket.MMR),
		strconv.FormatInt(joinedAt, 10),
		ticket.Region,
		isBot,
		DefaultBotQueueTTL.Milliseconds(),
	).Int()
	if err != nil {
		return fmt.Errorf("join queue: %w", err)
	}
	if joined == 0 {
		return ErrAlreadyQueued
	}
	return nil
}

func (s *RedisStore) ExpireBotTickets(ctx context.Context, region string) (int, error) {
	expired, err := s.expire.Run(
		ctx,
		s.client,
		[]string{queueKey(region), botExpiryIndexKey(region)},
		expiryBatchSize,
	).Int()
	if err != nil {
		return 0, fmt.Errorf("expire bot tickets for %s: %w", region, err)
	}
	return expired, nil
}

func (s *RedisStore) Metrics(ctx context.Context, region string) (model.QueueMetrics, error) {
	if _, err := s.ExpireBotTickets(ctx, region); err != nil {
		return model.QueueMetrics{}, err
	}
	queue := queueKey(region)
	entries, err := s.client.ZRangeWithScores(ctx, queue, 0, -1).Result()
	if err != nil {
		return model.QueueMetrics{}, fmt.Errorf("read queue metrics for %s: %w", region, err)
	}

	now := time.Now().UTC()
	buckets := makeMMRBuckets()
	if len(entries) == 0 {
		return model.QueueMetrics{
			Region:          region,
			QueueSize:       0,
			MMRDistribution: buckets,
			UpdatedAt:       now,
		}, nil
	}

	pipe := s.client.Pipeline()
	joinedAtCmds := make([]*redis.StringCmd, len(entries))
	for i, entry := range entries {
		playerID := fmt.Sprint(entry.Member)
		joinedAtCmds[i] = pipe.HGet(ctx, ticketKey(playerID), "joined_at")
		score := int(math.Round(entry.Score))
		bucket := score / mmrBucketWidth
		if bucket >= len(buckets) {
			bucket = len(buckets) - 1
		}
		buckets[bucket].Count++
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return model.QueueMetrics{}, fmt.Errorf("read ticket wait times: %w", err)
	}

	var totalWait time.Duration
	for i, cmd := range joinedAtCmds {
		joinedAt, err := cmd.Int64()
		if err != nil {
			return model.QueueMetrics{}, fmt.Errorf("read join time for queued ticket %q: %w", entries[i].Member, err)
		}
		wait := now.Sub(time.UnixMilli(joinedAt))
		if wait < 0 {
			wait = 0
		}
		totalWait += wait
	}

	return model.QueueMetrics{
		Region:          region,
		QueueSize:       int64(len(entries)),
		MMRDistribution: buckets,
		AverageWaitMS:   totalWait.Milliseconds() / int64(len(entries)),
		UpdatedAt:       now,
	}, nil
}

func (s *RedisStore) ActiveLobbies(ctx context.Context, region string) ([]model.Lobby, error) {
	indexKey := activeLobbyIndexKey(region)
	now, err := s.client.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("read Redis server time: %w", err)
	}
	if err := s.client.ZRemRangeByScore(ctx, indexKey, "-inf", strconv.FormatInt(now.UnixMilli(), 10)).Err(); err != nil {
		return nil, fmt.Errorf("remove expired lobby index entries: %w", err)
	}

	ids, err := s.client.ZRange(ctx, indexKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read active lobbies for %s: %w", region, err)
	}
	pipe := s.client.Pipeline()
	lobbyCmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		lobbyCmds[i] = pipe.Get(ctx, lobbyKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("read active lobby records: %w", err)
	}

	lobbies := make([]model.Lobby, 0, len(ids))
	for i, id := range ids {
		payload, err := lobbyCmds[i].Bytes()
		if err == redis.Nil {
			if removeErr := s.client.ZRem(ctx, indexKey, id).Err(); removeErr != nil {
				return nil, fmt.Errorf("remove missing lobby %s from index: %w", id, removeErr)
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read active lobby %s: %w", id, err)
		}
		var lobby model.Lobby
		if err := json.Unmarshal(payload, &lobby); err != nil {
			return nil, fmt.Errorf("decode active lobby %s: %w", id, err)
		}
		lobbies = append(lobbies, lobby)
	}
	return lobbies, nil
}

func (s *RedisStore) MatchOnce(ctx context.Context, region string) (*model.Match, error) {
	queue := queueKey(region)
	playerIDs, err := s.client.ZRange(ctx, queue, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read queue %s: %w", region, err)
	}
	if len(playerIDs) < playersPerMatch {
		return nil, nil
	}

	pipe := s.client.Pipeline()
	ticketCmds := make([]*redis.SliceCmd, len(playerIDs))
	for i, playerID := range playerIDs {
		ticketCmds[i] = pipe.HMGet(ctx, ticketKey(playerID), "mmr", "joined_at", "is_bot")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("read ticket data: %w", err)
	}

	tickets := make([]model.Ticket, 0, len(playerIDs))
	for i, playerID := range playerIDs {
		values, err := ticketCmds[i].Result()
		if err != nil || len(values) != 3 || values[0] == nil || values[1] == nil {
			continue
		}
		mmr, errMMR := strconv.Atoi(fmt.Sprint(values[0]))
		joinedAt, errTime := strconv.ParseInt(fmt.Sprint(values[1]), 10, 64)
		if errMMR != nil || errTime != nil {
			return nil, fmt.Errorf("invalid stored ticket %q", playerID)
		}
		tickets = append(tickets, model.Ticket{
			PlayerID: playerID,
			MMR:      mmr,
			Region:   region,
			JoinedAt: time.UnixMilli(joinedAt),
			IsBot:    fmt.Sprint(values[2]) == "1",
		})
	}
	if len(tickets) < playersPerMatch {
		return nil, nil
	}

	sort.Slice(tickets, func(i, j int) bool {
		return tickets[i].JoinedAt.Before(tickets[j].JoinedAt)
	})
	for anchorIndex, anchor := range tickets {
		tolerance := baseMMRGap + int(time.Since(anchor.JoinedAt).Seconds())*mmrPerWaitSec
		candidates := make([]model.Ticket, 0, playersPerMatch)
		for _, ticket := range tickets[anchorIndex:] {
			if abs(ticket.MMR-anchor.MMR) <= tolerance {
				candidates = append(candidates, ticket)
				if len(candidates) == playersPerMatch {
					break
				}
			}
		}
		if len(candidates) < playersPerMatch {
			continue
		}

		sort.Slice(candidates, func(i, j int) bool { return candidates[i].MMR < candidates[j].MMR })
		now := time.Now().UTC()
		match := &model.Match{
			ID:             fmt.Sprintf("%s-%d", region, now.UnixNano()),
			Region:         region,
			CreatedAt:      now,
			LobbyExpiresAt: now.Add(s.lobbyTTL),
			Players:        candidates,
			TeamOne:        make([]model.Ticket, 0, playersPerMatch/2),
			TeamTwo:        make([]model.Ticket, 0, playersPerMatch/2),
		}
		for i, ticket := range candidates {
			if i%2 == 0 {
				match.TeamOne = append(match.TeamOne, ticket)
			} else {
				match.TeamTwo = append(match.TeamTwo, ticket)
			}
		}

		lobby := model.Lobby{
			ID:        match.ID,
			MatchID:   match.ID,
			Region:    region,
			CreatedAt: match.CreatedAt,
			ExpiresAt: match.LobbyExpiresAt,
			Players:   match.Players,
			TeamOne:   match.TeamOne,
			TeamTwo:   match.TeamTwo,
		}
		lobbyJSON, err := json.Marshal(lobby)
		if err != nil {
			return nil, fmt.Errorf("encode active lobby: %w", err)
		}
		ttlSeconds := int64(math.Ceil(s.lobbyTTL.Seconds()))
		matchJSON, err := json.Marshal(match)
		if err != nil {
			return nil, fmt.Errorf("encode match history: %w", err)
		}
		args := []any{match.ID, playersPerMatch, ttlSeconds, string(lobbyJSON), match.ID, region, match.CreatedAt.UnixMilli(), string(matchJSON)}
		for _, ticket := range candidates {
			args = append(args, ticket.PlayerID)
		}
		claimed, err := s.claim.Run(
			ctx,
			s.client,
			[]string{queue, matchEventsKey, activeLobbyIndexKey(region), lobbyKey(match.ID), botExpiryIndexKey(region)},
			args...,
		).Int()
		if err != nil {
			return nil, fmt.Errorf("atomically claim match: %w", err)
		}
		if claimed != 1 {
			continue
		}
		return match, nil
	}
	return nil, nil
}

func queueKey(region string) string            { return "queue:" + region }
func ticketKey(playerID string) string         { return "ticket:" + playerID }
func lobbyKey(id string) string                { return "lobby:" + id }
func activeLobbyIndexKey(region string) string { return "lobbies:active:" + region }
func botExpiryIndexKey(region string) string   { return "queue:bot-expiry:" + region }

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func makeMMRBuckets() []model.MMRBucket {
	buckets := make([]model.MMRBucket, 0, mmrMaximum/mmrBucketWidth+1)
	for min := 0; min <= mmrMaximum; min += mmrBucketWidth {
		max := min + mmrBucketWidth - 1
		if max > mmrMaximum {
			max = mmrMaximum
		}
		buckets = append(buckets, model.MMRBucket{Min: min, Max: max})
	}
	return buckets
}

const matchEventsKey = "match:events"

const joinTicketLua = `
local status = redis.call('HGET', KEYS[2], 'status')
if status == 'queued' then
  return 0
end
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[2],
  'mmr', ARGV[2],
  'joined_at', ARGV[3],
  'region', ARGV[4],
  'status', 'queued',
  'is_bot', ARGV[5])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
if ARGV[5] == '1' then
  local now = redis.call('TIME')
  local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
  redis.call('ZADD', KEYS[3], now_ms + tonumber(ARGV[6]), ARGV[1])
end
return 1
`

const expireBotsLua = `
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
local player_ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms, 'LIMIT', 0, tonumber(ARGV[1]))
for _, player_id in ipairs(player_ids) do
  redis.call('ZREM', KEYS[1], player_id)
  redis.call('DEL', 'ticket:' .. player_id)
  redis.call('ZREM', KEYS[2], player_id)
end
return #player_ids
`

const claimMatchLua = `
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
local expected = tonumber(ARGV[2])
if #ARGV ~= expected + 8 then
  return redis.error_reply('invalid candidate count')
end
local min_mmr = nil
local max_mmr = nil
local min_tolerance = nil
for i = 1, expected do
  local player_id = ARGV[i + 8]
  local score = redis.call('ZSCORE', KEYS[1], player_id)
  local joined_at = redis.call('HGET', 'ticket:' .. player_id, 'joined_at')
  if not score or not joined_at then
    return 0
  end
  local mmr = tonumber(score)
  local wait_seconds = math.max(0, math.floor((now_ms - tonumber(joined_at)) / 1000))
  local tolerance = 50 + wait_seconds * 10
  min_mmr = min_mmr and math.min(min_mmr, mmr) or mmr
  max_mmr = max_mmr and math.max(max_mmr, mmr) or mmr
  min_tolerance = min_tolerance and math.min(min_tolerance, tolerance) or tolerance
end
if max_mmr - min_mmr > min_tolerance then
  return 0
end
for i = 1, expected do
  local player_id = ARGV[i + 8]
  redis.call('ZREM', KEYS[1], player_id)
  redis.call('ZREM', KEYS[5], player_id)
  redis.call('HSET', 'ticket:' .. player_id, 'status', 'matched', 'match_id', ARGV[1])
end
local ids = {}
for i = 1, expected do
  ids[i] = ARGV[i + 8]
end
redis.call('SET', KEYS[4], ARGV[4], 'EX', tonumber(ARGV[3]))
redis.call('ZADD', KEYS[3], now_ms + tonumber(ARGV[3]) * 1000, ARGV[1])
redis.call('XADD', KEYS[2], '*',
  'match_id', ARGV[1],
  'region', ARGV[6],
  'player_ids', table.concat(ids, ','),
  'created_at_ms', ARGV[7],
  'match_json', ARGV[8])
return 1
`
