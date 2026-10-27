package stepline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type Option func(*limiterConfig)

type limiterConfig struct {
	logger *slog.Logger
}

func WithLogger(logger *slog.Logger) Option {
	return func(c *limiterConfig) {
		c.logger = logger
	}
}

type Limiter struct {
	store    Store
	interval time.Duration
	logger   *slog.Logger
	reg      *registry

	closed  atomic.Bool
	closing chan struct{}

	subCancel context.CancelFunc
	subDone   chan struct{}

	shutdownOnce sync.Once
	shutdownErr  error
}

func NewLimiter(ctx context.Context, store Store, opts ...Option) (*Limiter, error) {
	cfg := limiterConfig{logger: slog.New(slog.DiscardHandler)}

	for _, opt := range opts {
		opt(&cfg)
	}

	interval := store.Interval()

	subCtx, subCancel := context.WithCancel(ctx)

	events, err := store.Subscribe(subCtx)
	if err != nil {
		subCancel()

		return nil, err
	}

	l := &Limiter{
		store:     store,
		interval:  interval,
		logger:    cfg.logger,
		reg:       &registry{},
		closing:   make(chan struct{}),
		subCancel: subCancel,
		subDone:   make(chan struct{}),
	}

	go runSubscriber(events, l.reg, l.logger, l.subDone)

	return l, nil
}

// Wait blocks the calling goroutine until it may make one call to the
// provider. A nil error means: make the call now. Call Wait once before
// every call.
//
// While blocked, the goroutine sleeps on a timer. Wait starts no goroutines.
// It makes one store call, and one more if ctx ends while waiting.
// It may return a few milliseconds late, never early.
// It can return earlier than first planned when callers ahead leave the
// queue, in this process or in any other Limiter on the same queue.
//
// ctx must have a deadline, and Wait never blocks past it:
//   - no deadline: [ErrNoDeadline] at once.
//   - the queue ahead does not fit the deadline: [ErrDeadlineTooShort] at once.
//   - ctx ends while waiting: ctx.Err(), and the callers behind move up.
//   - the store fails: an error wrapping [ErrStoreUnavailable] at once. Wait
//     never waits a failure out; you decide whether to call the provider.
//   - after [Limiter.Shutdown] starts: [ErrClosed] at once. A call already
//     waiting keeps its place and gets [ErrClosed] only if the Shutdown ctx
//     ends first.
func (l *Limiter) Wait(ctx context.Context) error {
	if l.closed.Load() {
		return ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	deadline, ok := ctx.Deadline()

	if !ok {
		return ErrNoDeadline
	}

	budget := max(time.Until(deadline), 0)

	t := l.reg.registerPending()

	result, err := l.store.Enqueue(ctx, budget)
	if err != nil {
		l.reg.removePending(t)

		return fmt.Errorf("stepline: enqueue: %w: %w", ErrStoreUnavailable, err)
	}

	if result.Ticket == 0 {
		l.reg.removePending(t)

		return ErrDeadlineTooShort
	}

	applied := l.reg.promote(t, result.Ticket, result.Version)
	wait := max(result.Wait-time.Duration(applied)*l.interval, 0)

	if wait == 0 {
		l.reg.remove(t)

		return nil
	}

	dueAt := time.Now().Add(wait)
	timer := time.NewTimer(wait)

	defer func() {
		timer.Stop()
		l.reg.remove(t)
	}()

	for {
		select {
		case <-timer.C:
			return nil

		case <-t.notify:
			n := t.pending.Swap(0)

			dueAt = dueAt.Add(-time.Duration(n) * l.interval)

			remaining := time.Until(dueAt)

			if remaining <= 0 {
				return nil
			}

			timer.Reset(remaining)

		case <-ctx.Done():
			cancelCtx := context.WithoutCancel(ctx)

			if cancelErr := l.store.Cancel(cancelCtx, result.Ticket); cancelErr != nil {
				l.logger.DebugContext(ctx, "stepline: cancel failed", "ticket", result.Ticket, "error", cancelErr)
			}

			return ctx.Err()

		case <-l.closing:
			return ErrClosed
		}
	}
}

func (l *Limiter) Shutdown(ctx context.Context) error {
	l.closed.Store(true)
	l.shutdownOnce.Do(func() {
		l.shutdownErr = l.doShutdown(ctx)
	})

	return l.shutdownErr
}

func (l *Limiter) doShutdown(ctx context.Context) error {
	defer func() {
		l.subCancel()
		<-l.subDone
	}()

	drained := l.reg.beginDrain()

	if drained == nil {
		return nil
	}

	select {
	case <-drained:
		return nil

	case <-ctx.Done():
		nums := l.reg.drainNumbers()
		close(l.closing)

		if len(nums) > 0 {
			if err := l.store.Cancel(context.WithoutCancel(ctx), nums...); err != nil {
				l.logger.DebugContext(ctx, "stepline: shutdown cancel failed", "error", err)
			}
		}

		return ctx.Err()
	}
}
