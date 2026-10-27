package e2e_test

import (
	"context"
	"testing"
	"time"
)

func TestTwoLimitersShareQueue(t *testing.T) {
	t.Parallel()

	t.Run("ConcurrentSpacing", func(t *testing.T) {
		t.Parallel()

		addr := newMiniredisAddr(t)
		clientA := newRedisClient(t, addr)
		clientB := newRedisClient(t, addr)

		limiterA, interval := newLimiter(t, clientA, "shared")
		limiterB, _ := newLimiter(t, clientB, "shared")

		firstCtx, firstCancel := context.WithTimeout(t.Context(), longDeadline)

		defer firstCancel()

		firstA := waitAsync(firstCtx, limiterA)

		time.Sleep(startGap)

		secondCtx, secondCancel := context.WithTimeout(t.Context(), longDeadline)

		defer secondCancel()

		firstB := waitAsync(secondCtx, limiterB)

		time.Sleep(startGap)

		thirdCtx, thirdCancel := context.WithTimeout(t.Context(), longDeadline)

		defer thirdCancel()

		secondA := waitAsync(thirdCtx, limiterA)

		time.Sleep(startGap)

		fourthCtx, fourthCancel := context.WithTimeout(t.Context(), longDeadline)

		defer fourthCancel()

		secondB := waitAsync(fourthCtx, limiterB)

		outcomes := collectSorted(firstA, firstB, secondA, secondB)

		assertAllNil(t, outcomes)
		assertSpacing(t, outcomes, interval)
	})

	t.Run("CrossPodCancel", func(t *testing.T) {
		t.Parallel()

		addr := newMiniredisAddr(t)
		clientA := newRedisClient(t, addr)
		clientB := newRedisClient(t, addr)

		limiterA, interval := newLimiter(t, clientA, "shared")
		limiterB, _ := newLimiter(t, clientB, "shared")

		anchorCtx, anchorCancel := context.WithTimeout(t.Context(), longDeadline)

		defer anchorCancel()

		anchor := waitAsync(anchorCtx, limiterA)

		time.Sleep(startGap)

		cancelledCtx, cancelledCancel := context.WithTimeout(t.Context(), longDeadline)

		defer cancelledCancel()

		cancelled := waitAsync(cancelledCtx, limiterA)

		time.Sleep(startGap)

		laterCtx, laterCancel := context.WithTimeout(t.Context(), longDeadline)

		defer laterCancel()

		later := waitAsync(laterCtx, limiterB)

		time.Sleep(startGap)
		cancelledCancel()

		anchorOutcome := <-anchor
		cancelledOutcome := <-cancelled
		laterOutcome := <-later

		assertNoError(t, anchorOutcome.err)
		assertErrorIs(t, cancelledOutcome.err, context.Canceled)
		assertNoError(t, laterOutcome.err)

		gap := laterOutcome.at.Sub(anchorOutcome.at)
		assertGapNear(t, gap, interval, eventTolerance)
	})
}
