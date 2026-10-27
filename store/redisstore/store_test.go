package redisstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/go-dev-stack/stepline"
	"github.com/go-dev-stack/stepline/store/redisstore"
)

func TestRealScriptsEndToEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := newMiniredisClient(t)

	store, err := redisstore.NewStore(ctx, client, "store-test", 1, stepline.PerSecond)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	first, err := store.Enqueue(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("first Enqueue failed: %v", err)
	}

	if first.Ticket != 1 {
		t.Fatalf("expected first ticket to be 1, got %d", first.Ticket)
	}

	if first.Wait != 0 {
		t.Fatalf("expected first wait to be 0, got %v", first.Wait)
	}

	if first.Version != 0 {
		t.Fatalf("expected first version to be 0, got %d", first.Version)
	}

	second, err := store.Enqueue(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("second Enqueue failed: %v", err)
	}

	if second.Ticket != 2 {
		t.Fatalf("expected second ticket to be 2, got %d", second.Ticket)
	}

	if second.Wait <= 0 {
		t.Fatalf("expected second wait to be > 0, got %v", second.Wait)
	}

	if cancelErr := store.Cancel(ctx, first.Ticket, second.Ticket); cancelErr != nil {
		t.Fatalf("Cancel failed: %v", cancelErr)
	}

	entries, err := client.XRange(ctx, "stepline:{store-test}:events", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE failed: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected exactly one stream entry, got %d", len(entries))
	}

	payload, ok := entries[0].Values["e"].(string)

	if !ok {
		t.Fatalf("expected string payload, got %T", entries[0].Values["e"])
	}

	const want = "cancelled 1 2"

	if payload != want {
		t.Fatalf("unexpected payload: got %q, want %q", payload, want)
	}
}

func newMiniredisClient(t *testing.T) *redis.Client {
	t.Helper()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { client.Close() })

	return client
}
