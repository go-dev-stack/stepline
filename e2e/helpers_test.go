package e2e_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/go-dev-stack/stepline"
	"github.com/go-dev-stack/stepline/store/redisstore"
)

const (
	rate   = 10
	period = stepline.PerSecond

	redisSetupTimeout    = 2 * time.Second
	longDeadline         = 5 * time.Second
	shortDeadline        = 10 * time.Millisecond
	shortShutdownTimeout = 20 * time.Millisecond
	startGap             = 20 * time.Millisecond
	timerTolerance       = 15 * time.Millisecond
	eventTolerance       = 30 * time.Millisecond
	generousUpperSlack   = 150 * time.Millisecond
	idleUpperBound       = 50 * time.Millisecond

	concurrentWaiters = 5
)

type waitOutcome struct {
	err error
	at  time.Time
}

func newMiniredisAddr(t *testing.T) string {
	t.Helper()

	mr := miniredis.RunT(t)

	return mr.Addr()
}

func newRedisClient(t *testing.T, addr string) *redis.Client {
	t.Helper()

	client := redis.NewClient(&redis.Options{Addr: addr})

	t.Cleanup(func() { client.Close() })

	return client
}

func newLimiter(t *testing.T, client redis.UniversalClient, key string) (*stepline.Limiter, time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), redisSetupTimeout)

	defer cancel()

	store, err := redisstore.NewStore(ctx, client, key, rate, period)
	assertNoError(t, err)

	limiter, err := stepline.NewLimiter(ctx, store)
	assertNoError(t, err)

	t.Cleanup(func() {
		_ = limiter.Shutdown(t.Context())
	})

	return limiter, store.Interval()
}

func waitAsync(ctx context.Context, l *stepline.Limiter) <-chan waitOutcome {
	ch := make(chan waitOutcome, 1)

	go func() {
		err := l.Wait(ctx)

		ch <- waitOutcome{err: err, at: time.Now()}
	}()

	return ch
}

func collectSorted(chans ...<-chan waitOutcome) []waitOutcome {
	outcomes := make([]waitOutcome, len(chans))

	for i, ch := range chans {
		outcomes[i] = <-ch
	}

	sort.Slice(outcomes, func(i, j int) bool {
		return outcomes[i].at.Before(outcomes[j].at)
	})

	return outcomes
}

func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertErrorIs(t *testing.T, err, target error) {
	t.Helper()

	if !errors.Is(err, target) {
		t.Fatalf("got error %v, want %v", err, target)
	}
}

func assertNotErrorIs(t *testing.T, err, target error) {
	t.Helper()

	if errors.Is(err, target) {
		t.Fatalf("got error %v, did not want %v", err, target)
	}
}

func assertAllNil(t *testing.T, outcomes []waitOutcome) {
	t.Helper()

	for i, o := range outcomes {
		if o.err != nil {
			t.Fatalf("outcome %d: unexpected error: %v", i, o.err)
		}
	}
}

func assertReturnsAtOnce(t *testing.T, elapsed time.Duration) {
	t.Helper()

	if elapsed > idleUpperBound {
		t.Fatalf("took %v, want under %v", elapsed, idleUpperBound)
	}
}

func assertGapNear(t *testing.T, gap, want, tolerance time.Duration) {
	t.Helper()

	if gap < want-tolerance {
		t.Fatalf("gap %v is less than %v", gap, want-tolerance)
	}

	if gap > want+generousUpperSlack {
		t.Fatalf("gap %v is more than %v", gap, want+generousUpperSlack)
	}
}

func assertSpacing(t *testing.T, outcomes []waitOutcome, interval time.Duration) {
	t.Helper()

	for i := 1; i < len(outcomes); i++ {
		gap := outcomes[i].at.Sub(outcomes[i-1].at)
		assertGapNear(t, gap, interval, timerTolerance)
	}
}
