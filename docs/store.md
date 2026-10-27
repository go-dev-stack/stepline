# Writing your own store

The Limiter talks to a `Store`. Redis, in `store/redisstore`, is the only
implementation, and the design is built around it. Another one is possible
but not planned.

```go
type Store interface {
	Enqueue(ctx context.Context, budget time.Duration) (EnqueueResult, error)
	Cancel(ctx context.Context, tickets ...int64) error
	Subscribe(ctx context.Context) (<-chan *CancelEvent, error)
	Interval() time.Duration
}
```

A store must:

- Run every operation as one atomic step. Two operations never interleave.
- Own the clock. Every time decision uses the store's clock. Replies carry
  durations and counters, never timestamps.
- Trim by clock: every operation first removes the tickets whose slot has
  passed and moves the head forward. A ticket sends nothing when it fires.
- Refuse a ticket in `Enqueue` when its wait is longer than `budget`: reply
  with ticket 0.
- In `Cancel`, take the numbers already sorted ascending and never sort
  them. Publish one event per call, version first, listing only the numbers
  actually removed.
- Deliver events to every reader in the order they were applied, each with
  its version. A reader can resume after the last event it read. Every
  reply carries the version it reflects.
- Return every failure as an error. Never block to wait a failure out.
- Own timeouts and retries, or leave them to the database client. The
  Limiter adds none: it passes the caller's context, or
  `context.WithoutCancel` for `Cancel`.
