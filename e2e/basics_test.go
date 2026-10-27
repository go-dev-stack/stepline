package e2e_test

import (
	"context"
	"testing"
	"time"

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

	assertNoError(t, err)
	assertReturnsAtOnce(t, elapsed)
}

func TestWaitErrDeadlineTooShort(t *testing.T) {
	t.Parallel()

	client := newRedisClient(t, newMiniredisAddr(t))
	limiter, _ := newLimiter(t, client, "provider")

	firstCtx, firstCancel := context.WithTimeout(t.Context(), longDeadline)

	defer firstCancel()

	assertNoError(t, limiter.Wait(firstCtx))

	shortCtx, shortCancel := context.WithTimeout(t.Context(), shortDeadline)

	defer shortCancel()

	start := time.Now()
	err := limiter.Wait(shortCtx)
	elapsed := time.Since(start)

	assertErrorIs(t, err, stepline.ErrDeadlineTooShort)
	assertNotErrorIs(t, err, context.DeadlineExceeded)
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

	assertErrorIs(t, err, stepline.ErrNoDeadline)
	assertReturnsAtOnce(t, elapsed)
}
