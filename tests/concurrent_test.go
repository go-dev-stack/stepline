package tests_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitConcurrentSpacing(t *testing.T) {
	t.Parallel()

	client := newRedisClient(t, newMiniredisAddr(t))
	limiter, interval := newLimiter(t, client, "provider")

	chans := make([]<-chan waitOutcome, concurrentWaiters)
	cancels := make([]context.CancelFunc, concurrentWaiters)

	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	for i := range concurrentWaiters {
		ctx, cancel := context.WithTimeout(t.Context(), longDeadline)

		cancels[i] = cancel
		chans[i] = waitAsync(ctx, limiter)
	}

	outcomes := collectSorted(chans...)

	for i, o := range outcomes {
		require.NoError(t, o.err, "outcome %d", i)
	}

	assertSpacing(t, outcomes, interval)
}

func TestWaitCancelShiftsLaterWaiter(t *testing.T) {
	t.Parallel()

	client := newRedisClient(t, newMiniredisAddr(t))
	limiter, interval := newLimiter(t, client, "provider")

	firstCtx, firstCancel := context.WithTimeout(t.Context(), longDeadline)

	defer firstCancel()

	first := waitAsync(firstCtx, limiter)

	time.Sleep(startGap)

	secondCtx, secondCancel := context.WithTimeout(t.Context(), longDeadline)

	defer secondCancel()

	second := waitAsync(secondCtx, limiter)

	time.Sleep(startGap)

	thirdCtx, thirdCancel := context.WithTimeout(t.Context(), longDeadline)

	defer thirdCancel()

	third := waitAsync(thirdCtx, limiter)

	time.Sleep(startGap)

	fourthCtx, fourthCancel := context.WithTimeout(t.Context(), longDeadline)

	defer fourthCancel()

	fourth := waitAsync(fourthCtx, limiter)

	time.Sleep(startGap)
	thirdCancel()

	firstOutcome := <-first
	secondOutcome := <-second
	thirdOutcome := <-third
	fourthOutcome := <-fourth

	require.NoError(t, firstOutcome.err)
	require.NoError(t, secondOutcome.err)
	require.ErrorIs(t, thirdOutcome.err, context.Canceled)
	require.NoError(t, fourthOutcome.err)

	gap := fourthOutcome.at.Sub(secondOutcome.at)
	assertGapNear(t, gap, interval, eventTolerance)
}
