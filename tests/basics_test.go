package tests_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-dev-stack/stepline"
)

func TestWaitIdleReturnsAtOnce(t *testing.T) {
	t.Parallel()

	client := newRedisClient(t, newMiniredisAddr(t))
	limiter, _ := newLimiter(t, client, "provider")

	ctx, cancel := context.WithTimeout(t.Context(), longDeadline)

	defer cancel()

	start := time.Now()
	err := limiter.Wait(ctx)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assertReturnsAtOnce(t, elapsed)
}

func TestWaitErrDeadlineTooShort(t *testing.T) {
	t.Parallel()

	client := newRedisClient(t, newMiniredisAddr(t))
	limiter, _ := newLimiter(t, client, "provider")

	firstCtx, firstCancel := context.WithTimeout(t.Context(), longDeadline)

	defer firstCancel()

	// Take the head slot.
	require.NoError(t, limiter.Wait(firstCtx))

	shortCtx, shortCancel := context.WithTimeout(t.Context(), shortDeadline)

	defer shortCancel()

	start := time.Now()
	err := limiter.Wait(shortCtx)
	elapsed := time.Since(start)

	// deadline=10ms < interval=100ms
	require.ErrorIs(t, err, stepline.ErrDeadlineTooShort)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	assertReturnsAtOnce(t, elapsed)
}

func TestWaitErrNoDeadline(t *testing.T) {
	t.Parallel()

	client := newRedisClient(t, newMiniredisAddr(t))
	limiter, _ := newLimiter(t, client, "provider")

	ctx := context.WithoutCancel(t.Context())

	start := time.Now()
	err := limiter.Wait(ctx)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, stepline.ErrNoDeadline)
	assertReturnsAtOnce(t, elapsed)
}
