package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"matchmaker/internal/httpapi"
	"matchmaker/internal/matchmaking"
	"matchmaker/internal/realtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("REDIS_URL is required")
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("parse REDIS_URL: %v", err)
	}

	client := redis.NewClient(options)
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("connect to Redis: %v", err)
	}

	store := matchmaking.NewRedisStore(client)
	hub := realtime.NewHub()
	api := httpapi.NewServer(store, hub, os.Getenv("FRONTEND_ORIGIN"))

	go hub.Run(ctx)
	go runMatcher(ctx, store, hub)

	addr := os.Getenv("ADDR")
	if addr == "" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		addr = ":" + port
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown: %v", err)
		}
	}()

	log.Printf("matchmaker listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server: %v", err)
	}
}

func runMatcher(ctx context.Context, store *matchmaking.RedisStore, hub *realtime.Hub) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, region := range matchmaking.Regions {
				if _, err := store.ExpireBotTickets(ctx, region); err != nil {
					log.Printf("expire bot tickets in region %s: %v", region, err)
				}
				match, err := store.MatchOnce(ctx, region)
				if err != nil {
					log.Printf("match region %s: %v", region, err)
					continue
				}
				if match != nil {
					hub.Broadcast(match)
				}
			}
		}
	}
}
