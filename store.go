package stepline

import (
	"context"
	"time"
)

type EnqueueResult struct {
	Ticket  int64
	Wait    time.Duration
	Version int64
}

type Store interface {
	Enqueue(ctx context.Context, budget time.Duration) (EnqueueResult, error)
	Cancel(ctx context.Context, tickets ...int64) error
	Subscribe(ctx context.Context) (<-chan *CancelEvent, error)
	Interval() time.Duration
}
