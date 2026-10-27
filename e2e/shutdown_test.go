package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-dev-stack/stepline"
)

func TestShutdown(t *testing.T) {
	t.Parallel()

	t.Run("WaitAfterShutdownReturnsErrClosed", func(t *testing.T) {
		t.Parallel()

		client := newRedisClient(t, newMiniredisAddr(t))
		limiter, _ := newLimiter(t, client, "provider")

		shutdownCtx, shutdownCancel := context.WithTimeout(t.Context(), longDeadline)

		defer shutdownCancel()

		assertNoError(t, limiter.Shutdown(shutdownCtx))

		waitCtx, waitCancel := context.WithTimeout(t.Context(), longDeadline)

		defer waitCancel()

		start := time.Now()
		err := limiter.Wait(waitCtx)
		elapsed := time.Since(start)

		assertErrorIs(t, err, stepline.ErrClosed)
		assertReturnsAtOnce(t, elapsed)
	})

	t.Run("ShortDeadlineCancelsInFlightWaiter", func(t *testing.T) {
		t.Parallel()

		client := newRedisClient(t, newMiniredisAddr(t))
		limiter, _ := newLimiter(t, client, "provider")

		firstCtx, firstCancel := context.WithTimeout(t.Context(), longDeadline)

		defer firstCancel()

		assertNoError(t, limiter.Wait(firstCtx))

		secondCtx, secondCancel := context.WithTimeout(t.Context(), longDeadline)

		defer secondCancel()

		second := waitAsync(secondCtx, limiter)

		time.Sleep(startGap)

		shutdownCtx, shutdownCancel := context.WithTimeout(t.Context(), shortShutdownTimeout)

		defer shutdownCancel()

		shutdownErr := limiter.Shutdown(shutdownCtx)
		secondOutcome := <-second

		assertErrorIs(t, shutdownErr, context.DeadlineExceeded)
		assertErrorIs(t, secondOutcome.err, stepline.ErrClosed)
		assertNotErrorIs(t, secondOutcome.err, context.DeadlineExceeded)
	})
}
