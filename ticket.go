package stepline

import "sync/atomic"

type ticket struct {
	number       int64
	replyVersion int64
	pending      atomic.Int64
	notify       chan struct{}
	backlog      []*CancelEvent
	idx          int
}

func newTicket() *ticket {
	return &ticket{notify: make(chan struct{}, 1)}
}
