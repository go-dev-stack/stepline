package redisstore_test

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-dev-stack/stepline"
	"github.com/go-dev-stack/stepline/store/redisstore"
)

func TestRealScriptsEndToEnd(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newMiniredisClient(t)

	store, err := redisstore.NewStore(ctx, client, "store-test", 1, stepline.PerSecond)
	require.NoError(t, err)

	first, err := store.Enqueue(ctx, 5*time.Second)
	require.NoError(t, err)

	assert.Equal(t, int64(1), first.Ticket)
	assert.Zero(t, first.Wait)
	assert.Zero(t, first.Version)

	second, err := store.Enqueue(ctx, 5*time.Second)
	require.NoError(t, err)

	assert.Equal(t, int64(2), second.Ticket)
	assert.Positive(t, second.Wait)

	require.NoError(t, store.Cancel(ctx, first.Ticket, second.Ticket))

	entries, err := client.XRange(ctx, "stepline:{store-test}:events", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	payload, ok := entries[0].Values["e"].(string)
	require.True(t, ok, "expected string payload, got %T", entries[0].Values["e"])

	assert.Equal(t, "cancelled 1 2", payload)
}

func newMiniredisClient(t *testing.T) *redis.Client {
	t.Helper()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { client.Close() })

	return client
}
