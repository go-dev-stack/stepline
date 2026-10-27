package stepline

import (
	"slices"
	"sync"
)

const (
	compactDeadFloor       = 1024
	compactShrinkCapFactor = 4
	compactShrinkCapFloor  = 1024
)

type registry struct {
	mu       sync.Mutex
	tickets  []*ticket
	pending  []*ticket
	head     int
	live     int
	dead     int
	draining bool
	drained  chan struct{}
}

func (r *registry) registerPending() *ticket {
	t := newTicket()

	r.mu.Lock()

	r.pending = append(r.pending, t)
	r.mu.Unlock()

	return t
}

func (r *registry) removePending(t *ticket) {
	r.mu.Lock()
	r.removePendingLocked(t)
	r.checkDrainedLocked()
	r.mu.Unlock()
}

func (r *registry) removePendingLocked(t *ticket) {
	for i, p := range r.pending {
		if p == t {
			r.pending = append(r.pending[:i], r.pending[i+1:]...)

			return
		}
	}
}

func (r *registry) promote(t *ticket, number, version int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removePendingLocked(t)

	t.number = number
	t.replyVersion = version

	var applied int64

	for _, ev := range t.backlog {
		if ev.Version <= version {
			continue
		}

		below, _ := slices.BinarySearch(ev.Tickets, number)

		applied += int64(below)
	}

	t.backlog = nil
	r.insertLocked(t)

	return applied
}

func (r *registry) insertLocked(t *ticket) {
	i := len(r.tickets)

	for i > 0 {
		c := r.tickets[i-1]

		if c == nil {
			i--

			continue
		}

		if c.number < t.number {
			break
		}

		i--
	}

	r.tickets = append(r.tickets, nil)

	for k := len(r.tickets) - 1; k > i; k-- {
		r.tickets[k] = r.tickets[k-1]

		if r.tickets[k] != nil {
			r.tickets[k].idx = k
		}
	}

	r.tickets[i] = t
	t.idx = i
	r.live++
}

func (r *registry) remove(t *ticket) {
	r.mu.Lock()

	if t.idx < len(r.tickets) && r.tickets[t.idx] == t {
		r.tickets[t.idx] = nil

		if t.idx == r.head {
			r.head++

			for r.head < len(r.tickets) && r.tickets[r.head] == nil {
				r.head++
			}
		}

		r.dead++

		r.live--
		r.compactLocked()
		r.checkDrainedLocked()
	}

	r.mu.Unlock()
}

func (r *registry) checkDrainedLocked() {
	if r.draining && r.live == 0 && len(r.pending) == 0 {
		close(r.drained)

		r.draining = false
	}
}

func (r *registry) beginDrain() chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.live == 0 && len(r.pending) == 0 {
		return nil
	}

	r.draining = true
	r.drained = make(chan struct{})

	return r.drained
}

func (r *registry) compactLocked() {
	if r.dead <= r.live || r.dead <= compactDeadFloor {
		return
	}

	if cap(r.tickets) > compactShrinkCapFactor*r.live && cap(r.tickets) > compactShrinkCapFloor {
		fresh := make([]*ticket, r.live)
		w := 0

		for i := r.head; i < len(r.tickets); i++ {
			if t := r.tickets[i]; t != nil {
				fresh[w] = t
				t.idx = w
				w++
			}
		}

		r.tickets = fresh
	} else {
		w := 0

		for i := r.head; i < len(r.tickets); i++ {
			if t := r.tickets[i]; t != nil {
				r.tickets[w] = t
				t.idx = w
				w++
			}
		}

		clear(r.tickets[w:])

		r.tickets = r.tickets[:w]
	}

	r.head = 0
	r.dead = 0
}

func (r *registry) applyCancel(ev *CancelEvent) (notified, skipped int) {
	r.mu.Lock()

	j := 0
	n := len(ev.Tickets)

	for i := r.head; i < len(r.tickets); i++ {
		t := r.tickets[i]

		if t == nil {
			continue
		}

		for j < n && ev.Tickets[j] < t.number {
			j++
		}

		if j > 0 && t.replyVersion < ev.Version {
			t.pending.Add(int64(j))

			// notify has a buffer of 1, so the ticket blocking applyCancel.
			// At most 1 signal is sufficient:
			// receiver should be invoked single time and will process all the events.
			select {
			case t.notify <- struct{}{}:
			default:
			}

			notified++
		} else {
			skipped++
		}
	}

	for _, p := range r.pending {
		p.backlog = append(p.backlog, ev)
	}

	r.mu.Unlock()

	return notified, skipped
}

func (r *registry) drainNumbers() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	nums := make([]int64, 0, r.live)

	for i := r.head; i < len(r.tickets); i++ {
		if t := r.tickets[i]; t != nil {
			nums = append(nums, t.number)
		}
	}

	r.tickets = nil
	r.pending = nil
	r.head = 0
	r.live = 0
	r.dead = 0

	return nums
}
