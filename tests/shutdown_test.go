package tests_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

		require.NoError(t, limiter.Shutdown(shutdownCtx))

		waitCtx, waitCancel := context.WithTimeout(t.Context(), longDeadline)

		defer waitCancel()

		start := time.Now()
		err := limiter.Wait(waitCtx)
		elapsed := time.Since(start)

		require.ErrorIs(t, err, stepline.ErrClosed)
		assertReturnsAtOnce(t, elapsed)
	})

	t.Run("ShortDeadlineCancelsInFlightWaiter", func(t *testing.T) {
		t.Parallel()

		client := newRedisClient(t, newMiniredisAddr(t))
		limiter, _ := newLimiter(t, client, "provider")

		firstCtx, firstCancel := context.WithTimeout(t.Context(), longDeadline)

		defer firstCancel()

		require.NoError(t, limiter.Wait(firstCtx))

		secondCtx, secondCancel := context.WithTimeout(t.Context(), longDeadline)

		defer secondCancel()

		second := waitAsync(secondCtx, limiter)

		time.Sleep(startGap)

		shutdownCtx, shutdownCancel := context.WithTimeout(t.Context(), shortShutdownTimeout)

		defer shutdownCancel()

		shutdownErr := limiter.Shutdown(shutdownCtx)
		secondOutcome := <-second

		require.ErrorIs(t, shutdownErr, context.DeadlineExceeded)
		require.ErrorIs(t, secondOutcome.err, stepline.ErrClosed)
		require.NotErrorIs(t, secondOutcome.err, context.DeadlineExceeded)
	})
}
