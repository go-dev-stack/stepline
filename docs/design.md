# Design

How stepline works: the queue, the Redis store and the Limiter. Covers the
happy path, failure handling is not designed yet. Terms are defined in
the [glossary](../README.md#glossary). Open work is in the
[issues](https://github.com/go-dev-stack/stepline/issues).

## Roles

| Role | How many | Lives | Owns |
|---|---|---|---|
| Store | one Redis, shared by every process | forever | the queue, the clock, the events. `Store` is the interface name; Redis is its only implementation, and this document describes the Redis behaviour directly |
| Limiter | one per process per limiter key | from construction to `Shutdown(ctx)` | the Store, the interval read from it, a `*slog.Logger` (Debug only, discard when absent), the Subscriber, the registry of tickets waiting in this process. Safe for concurrent use by any number of callers |
| Subscriber | one goroutine per Limiter | as long as the Limiter | the stream reader connection and the ID of the last entry it processed; parses each event once and applies it to the registry with `applyCancel`, adding a count to every affected ticket's counter and poking it (see [Fan-out](#fan-out-how-one-event-reaches-the-tickets-of-a-process)) |
| Ticket | one per `Wait(ctx)` call | from `Wait` start to `Wait` return | its number, dueAt, one timer, its entry in the registry, the `ticket` struct (counter, notify channel, backlog). It is the calling goroutine itself, blocked inside `Wait` until its slot. That goroutine already exists before `Wait` is called: a gin handler goroutine, an asynq worker, or one of the errgroup goroutines that call the client in parallel. Inside `Wait` there are no goroutines either, only one `select` over four channels: the timer, the notify channel, `ctx.Done()`, and the Limiter's closing channel |
| Caller | outside the package | | the goroutine that calls `Wait` before each request to the provider. How it is wired into the caller's code is the caller's choice, not designed here |

## Limiter key

One provider budget = one limiter key = one queue in the Store. The key
names the budget, not the caller: every process that spends the same
budget uses the same key and therefore shares one queue.
Example: `slack` for one Slack API key used by the whole organization;
`slack:chat.postMessage` when the provider limits each method separately,
so each method gets its own queue. Two keys for one
budget is a mistake (docs/429.md).

## Queue lifecycle

Head is always the slot of the first ticket still waiting, never the time
of the last one that fired. Every case happens inside a script, so "now"
is Redis time at that script.

1. **Birth.** The very first enqueue: no `head` key, empty queue. Head =
   now, position 0, wait 0. The ticket fires at once.
2. **Join.** An enqueue while the first ticket is still waiting for its
   timer, so its slot is still in the future and nothing is trimmed. The
   newcomer goes to the back: position = number of waiting tickets, slot =
   `head + position * interval`, wait = slot - now. Head unchanged, the
   first ticket keeps its slot.
3. **Advance.** The first ticket's slot passed; it fired on its own timer.
   The next script trims it (see [Trim](#trim)) and moves head one
   interval forward, to the next ticket's slot. When 12's slot passes,
   head becomes 13's slot. One step per passed slot.
4. **Cancel.** A waiting ticket leaves early. Its Limiter sends `Cancel`
   (see [Operations](#operations)); the Store removes the number and
   publishes the event (see [Events and the version](#events-and-the-version));
   every Limiter with a higher number subtracts one interval from its
   dueAt (see [Limiter](#limiter), rule 2). Head untouched: the first
   waiting ticket keeps its slot, everything behind moves one slot
   earlier.
5. **Drain.** The last ticket's slot passed. The next script trims it:
   empty queue, head = that slot + interval. Head is now a reservation of
   the next slot, still in the future.
6. **Resume.** An enqueue arrives while the drained head is still in the
   future. Position 0, wait = head - now. Last ticket fired 0.4s ago: wait
   0.6s. Slots continue one interval after the last fire.
7. **Expire.** The drained head passes with no enqueue. Nothing happens at
   that moment, Redis has no timers. Head is now a stale timestamp.
8. **Restart.** An enqueue finds an empty queue and head in the past.
   Head = now, position 0, wait 0. Same as Birth, only the stale `head`
   key is overwritten. Head starts over.

Resume and Restart are told apart by one comparison in the trim's idle
rule: empty queue and head < now means Restart, otherwise Resume.

## Store

### State, per limiter key

Every key starts with the hardcoded prefix `stepline:`, so all limiter
data is findable with one `SCAN stepline:*`, for example by a future
background job that trims occasionally. All keys of one limiter share a
hash tag, so they live in one Redis hash slot and one script can touch
them all. Example for key `slack`:

| Key | Type | What it holds |
|---|---|---|
| `stepline:{slack}:queue` | sorted set | the waiting tickets: member = number as string, score = number |
| `stepline:{slack}:head` | string | head, Redis time in microseconds |
| `stepline:{slack}:seq` | string | the next ticket number |
| `stepline:{slack}:ver` | string | the version |
| `stepline:{slack}:events` | stream | one entry per event, kept for at least the last 60 seconds |

1. The waiting tickets: the numbers that have not fired yet, ordered by
   number. The order is the FIFO, the position is the index in it. A number
   enters on enqueue and leaves by trim or by cancel. When 17 enqueues the
   set is 13, 14, 15, 16, 17 and 17 has position 4. After 15 cancels and
   13, 14 are trimmed, the set is 16, 17.
2. Head: one timestamp in its own key. It is not in the set; the set holds
   numbers only, and the ticket at position 0 is simply the lowest number,
   with no time of its own. Head is that ticket's slot. Lifecycle in
   [Queue lifecycle](#queue-lifecycle). Head T+1 means 13 fires at T+1 and
   17, at position 4, at T+5. After 13 and 14 are trimmed it is T+3.
3. The next ticket number: `INCR` on every enqueue. Never reused, never
   reset. A number is a ticket's identity and its place in the order; a
   reused number would let an old, slow ticket cancel a new one. After 17
   is handed out the counter holds 18, whatever happens to 17.
4. The version: `INCR` on every change that produces an event, today only
   cancel. See Events and the version.

### Clock

`TIME` inside the script is the only clock; it returns microseconds and
every time value in the Store is kept in microseconds. Absolute times never
leave Redis; replies and events carry durations and counters only, so pod
clocks and time zones do not change the result. The interval comes in as a
script argument from the Limiter, in microseconds, rounded up to a multiple
of 100us: 2.67ms becomes 2.7ms. Finer than that is not promised, coarser
would throw away throughput for nothing.

### Trim

Every script starts with a trim. Nothing else triggers one: Redis has no
timers and never recalculates on its own. A fired ticket may sit in the set
until the next enqueue or cancel arrives. That is harmless, because only
the scripts read the Redis values and every script trims first. The Limiter and the
Store agree on the schedule in advance, so a ticket sends nothing when it
fires.

- `passed` = how many slots are at or before now. While head
  is still in the future (Join) it is 0; once head has passed it is
  `floor((now - head) / interval) + 1`. Head T+1 at T+0.2: 0. Head T+1 at
  T+2.5: 2, the slots T+1 and T+2.
- `popped = ZPOPMIN queue min(passed, ZCARD)`, then
  `head += popped * interval`. `ZCARD` is the number of members in the
  set, `ZPOPMIN queue n` removes and returns the `n` lowest numbers, the
  front of the queue. The `+ interval` is added once per popped ticket,
  for the slot that ticket consumed, never for the caller.
- Idle rule: if the queue is now empty and head < now, head = now. A
  missing `head` key counts as head < now.

A ticket whose owner died is trimmed the same way, its slot passes unused.

What head means depends on the set size. Head lives outside the set (see
[State](#state-per-limiter-key)); the state is the pair (set, head), never
head alone:

- Set not empty: after the trim, head is the first waiting ticket's slot,
  and that slot is in the future by construction, everything at or before
  now was popped. This is why the trim is safe in every script, cancel
  included: it only moves head past slots that were consumed, and lands on
  the slot of the next waiting ticket. Queue 15, 16, 17 with head T+3,
  cancel at T+2.5: nothing passed, head stays T+3, `ZREM 15`, event, head
  is now 16's slot because 16 moved one interval earlier on the event.
- Set empty: head means "the next slot" only while it is in the future
  (Drain, Resume). Once it is in the past it means nothing (Expire), and
  only the next script tells, by comparing it with now. Tickets 15, 16, 17
  with slots T+3, T+4, T+5, untouched until a late cancel at T+30: passed
  is 28 but only 3 exist, popped 3, head T+6, set empty, T+6 < T+30, head
  = T+30, `ZREM` finds nothing, no event. Head = now on an empty set never
  delays anyone: an enqueue at T+30.4 sees empty and T+30 < now, Restart,
  wait 0; at exactly T+30.0 it is Resume with wait = head - now = 0.
- After a trim, head < now already implies an empty set, since a
  non-empty set leaves head in the future. The `ZCARD == 0` in the idle
  rule is the same fact spelled out; keep it explicit.

The trim inside the cancel script is required, not optional. Without it, a
cancel arriving after the ticket's slot already passed would `ZREM` a
ticket that has fired and publish an event for it, and every ticket behind
would move one interval earlier for a slot that was really consumed, two
calls in one interval. The trim turns that case into "already gone, no
event".

Lazy init. No key is created at Limiter construction; the scripts create
what is missing on first use (`head` by the idle rule, the counters by
`INCR`). This is deliberate: keys can vanish while the Limiter lives, for
example a Redis failover that loses them. With init-time keys every ticket
after that would fail on a missing key; with lazy init the next enqueue
simply starts over from an empty queue.

### Operations

Each operation is one Lua script, so it sees and changes the state as one
step and two operations never interleave. The trim lives in
`lua/trim.lua` and is put in front of `enqueue.lua` and `cancel.lua` when
the scripts load, so it exists once. `redis.replicate_commands()` is not
called: `TIME` works in Lua without it, and it is deprecated in Redis 7.

- `Enqueue(interval, budget)`. `budget` is the time left until the
  caller's deadline in microseconds, always present, never negative: a
  context without a deadline is refused by the Limiter before any Store
  call. Trim, then `position = ZCARD`, slot = `head + position *
  interval`, `wait = max(0, slot - now)`. If `wait > budget` the ticket is
  not admitted: no `INCR seq`, no `ZADD`, reply `[0, wait, version]`,
  number 0 meaning "would not fit"; nobody else is touched. Otherwise
  `n = INCR seq`, `ZADD queue n n`, reply `[n, wait, version]`. Example:
  `[17, 4800000, 40]`. One round trip either way.
- `Cancel(tickets...)`. One or many numbers in one call, ascending; the
  caller sorts, the Store never does. Trim, then `ZREM queue` for every
  given number in the given order. If at least one was still there:
  `v = INCR ver` once, `XADD events MINID ~ <now - 60s> * e "cancelled v
  n n ..."` listing only the numbers actually removed, in the given
  order, so the payload is ascending by construction. One field, `e`,
  holding the whole string. `MINID ~` trims entries older than 60
  seconds, whatever the event rate; the `~` lets Redis trim in whole
  internal nodes, so some older entries may remain a little longer and the
  trim stays cheap. Retention is stated in time, not count, because
  cancels are not bounded by the release rate. If none was there:
  nothing, no event.
  The reply carries no information; the caller only needs to know the
  request reached Redis. A single ticket cancel and the batch sent by
  `Shutdown` are the same script.
- `Subscribe()`. One stream reader per process: a loop of `XREAD BLOCK 0
  STREAMS stepline:{key}:events <lastID>`, starting at `$` (entries added
  after construction; nothing before it matters, there are no tickets
  yet), then always the ID of the last entry processed. A broken
  connection is re-established by the client and the loop continues from
  that ID, so no entry is skipped while it still exists in the stream.
  `NewLimiter` runs one non-blocking read first, the loud check.

### Events and the version

One event kind today, the string `"cancelled <version> <ticket> ..."`,
version first so the list can be any length: `"cancelled 41 15"` for one
cancel, `"cancelled 42 15 18 22"` for a batch from `Shutdown`. There is no
event on fire and none on enqueue. 60k numbers make about 420KB in one
message, acceptable; range compression is in [Ideas](https://github.com/go-dev-stack/stepline/issues/18).

Two orderings, two jobs. The version counter is what tickets use: it
grows by exactly one per event, so it is also a gap detector. The stream
ID is only the reader's resume position. The Subscriber compares each
event's version with the previous one; a jump means entries were trimmed
away before the reader got them, possible only after a disconnect longer
than the retention. For now that logs a Warn and continues; replaying
from the stream with `XRANGE` when the entries still exist, or resyncing
the process's tickets when they do not, is [#3](https://github.com/go-dev-stack/stepline/issues/3).

A reply is a snapshot: it reflects every change up to the version it
names. An event is one change, stamped with the version at which Redis
applied it. The two travel on different connections, so their order at a
Limiter is not guaranteed. A cancel applied just before my enqueue is
already inside my wait. If its event reaches me after the reply, I would
subtract one interval twice and fire one slot early. The rule that makes
this exact: ignore events at or below my reply's version.

## Limiter

### Ticket rules

Per ticket, one value: dueAt.

0. Before anything: a context that is already done returns `ctx.Err()`;
   a context without a deadline returns `ErrNoDeadline`. Neither touches
   the Store. The deadline is the only bound the design relies on, so it
   is mandatory; a caller that wants "as long as it takes" says so with
   an explicit long timeout.
1. Enqueue reply: number 0 means the wait does not fit the caller's
   deadline, return `ErrDeadlineTooShort`, nothing to clean up. Otherwise
   dueAt = now + wait, remember the reply version.
2. Event `cancelled` with version above my reply's: count the listed
   numbers below mine, dueAt = dueAt - count * interval.
3. Timer at dueAt: `Wait` returns nil. Nothing is sent to the Store; the
   ticket stops its timer and leaves the registry.
4. `ctx.Done()`: send `Cancel` on `context.WithoutCancel(ctx)`, so the
   cancel still reaches Redis after the caller's context died; the Redis
   client's own timeouts bound the call. Then return `ctx.Err()`.
5. Any Store error: return the exported unavailable error, never block.
6. `Shutdown(ctx)` on the Limiter: new `Wait` calls return `ErrClosed` at
   once. Registered tickets keep waiting while `ctx` is alive and finish
   on their own. When `ctx` ends, or at once if it is already done, the
   Limiter sends every remaining number to the Store in one `Cancel`,
   ascending, wakes those tickets with `ErrClosed`, stops the Subscriber
   and closes the subscription. Returns `ctx.Err()` if it had to cut
   tickets off, nil if they all finished. Idempotent through
   `sync.Once`. It waits on a `drained` channel that the registry closes
   when its last live or pending ticket is removed, so it never polls.
   There is no separate `Close`.

The `select` inside `Wait` has four cases: the ticket's timer, its notify
channel, `ctx.Done()`, and one Limiter-wide closing channel that the hard
stop of `Shutdown` closes. One `time.Timer` per ticket, `Reset` only when
dueAt changes. After a wake-up subtracts intervals, if the remaining time
is at or below zero `Wait` returns nil right there; it never resets the
timer into the past. A past-time timer would only join the random choice
between ready select cases, and the due slot must win over another
wake-up, `ctx.Done()` or closing.

### Fan-out: how one event reaches the tickets of a process

Every happy-path event means the same thing for a ticket, "subtract some
intervals", so the Subscriber does not forward events, it forwards a
count.

The event. The Subscriber parses the payload `cancelled <version> <n>
...` once into one heap value `{version int64, numbers []int64}` and
hands the same pointer to every consumer, all read-only. `numbers` is
ascending because the caller of `Cancel` sorts and the Store publishes in
the given order; the Subscriber never sorts and never copies it.

The `ticket` struct. One per waiting ticket: `number`, `replyVersion`, `pending`
(an atomic counter of intervals to subtract), `notify` (a channel of size
one, used only as a wake-up), `backlog` (a slice of event pointers, used
only before the reply arrives), `idx` (its index in `tickets`, so a
removal is O(1) and needs no scan). The counter carries the value, the channel
only says "look"; a `chan int` of size one could not add up two wake-ups
without blocking the Subscriber or losing a count. Pointers, not structs,
in the backlog: the event is one shared object and the GC follows the
same references either way. About 150 bytes per ticket, 9MB at 60k.

The registry. One structure owned by the Limiter, guarded by one mutex:

- `tickets []*ticket`, ordered by number.
- `pending []*ticket`, tickets whose enqueue request is in flight, number
  and version unknown. A handful at any moment.
- `head int`, index of the first live entry in `tickets`; everything
  before it is consumed.
- `live int`, `dead int` (holes plus consumed prefix).

Two parties touch it. Tickets, in their callers' goroutines: register,
move from `pending` to `tickets` on reply, leave on fire, cancel or
`ErrClosed`. The Subscriber goroutine: one pass per event. They meet only
through the mutex; the Subscriber never calls into a ticket, a ticket
never sees an event.

Registration happens before the enqueue request, not after the reply:
create the ticket with number and version unknown, append to `pending`,
then send the request. While the reply is in flight the Subscriber
appends every event pointer to that ticket's backlog instead of counting.
When the reply arrives the ticket fills in number and version, applies
the backlog events whose version is above its own (count the numbers
below its number, subtract), clears the backlog, and moves from `pending`
into `tickets`. An event processed before the ticket existed was applied
before the enqueue, so its version is below the reply's and is correctly
ignored. This is exact, needs no size, and closes the only way a ticket
could miss a shift. A missed shift is not "late and safe": the ticket
would fire in the slot that now belongs to the ticket behind it, two
calls in one interval. The backlog has no cap; it grows by one pointer
per event for one round trip, and a reply that never comes ends with the
unavailable error from the client's timeout, backlog dropped.

The move into `tickets` is an insert, not an append. Replies of two
tickets enqueued at the same moment can cross on different pooled
connections, so 19 may arrive before 18. Scan back from the tail,
skipping holes, to the first number below mine, shift the rest right by
one, place the ticket. A binary search does not work on a slice with nil
holes. Crossing only happens among tickets enqueued within the same few
milliseconds, so the insert is always near the end and the shift is a
few pointers. The slice grows by `append`; there is no fixed size.

`applyCancel`, one event, under the lock for the whole pass over both slices
so the event is applied to one consistent snapshot. It is the merge step
of merge sort, a two-pointer linear merge of two sorted lists:

1. `i` runs over `tickets` from `head`, skipping nil holes; `j` runs over
   `numbers`, both starting at 0.
2. While `numbers[j]` is below `tickets[i].number`, advance `j`. Now `j`
   is exactly how many cancelled numbers are below this ticket.
3. If `j` is above zero and `tickets[i].replyVersion` is below the
   event's version: `pending.Add(j)`, non-blocking send on `notify`. If
   the channel already holds a wake-up the send is skipped; the counter
   keeps the truth.
4. Advance `i`. `j` never moves back, because the next ticket has a
   bigger number.
5. Then over `pending`: append the event pointer to each backlog.

Each index moves forward only, so one event costs `len(tickets) +
len(numbers)` steps plus one add and one send per affected ticket: 6k
tickets and a 6k batch is 12k steps. No search, no map iteration. With
tickets 13, 16, 17, 20, 22 and numbers 14, 15, 21: at 13 `j` stays 0; at
16 `j` moves to 2; at 17 and 20 it stays 2; at 22 it moves to 3.

The wake, in the ticket's own goroutine, after the lock is released: swap
`pending` to zero, subtract that many intervals from dueAt, return nil if
already due, otherwise `Reset` the timer.

Registry maintenance. Removal never shifts anything: a ticket leaving
from the front sets its entry to nil and advances `head`; a ticket leaving
from the middle sets its entry to nil and becomes a hole that `applyCancel` and the
insert skip. Setting to nil frees the ticket at once. When `dead > live`
and `dead > 1024`, the goroutine that just removed compacts, still under
the lock. First decision, shrink or not: if `cap(tickets) > 4 * live` and
`cap > 1024`, allocate a fresh slice of length `live`, copy the non-nil
pointers into it and drop the old array whole, so a burst does not pin
memory. Otherwise in place: one loop copies every non-nil pointer forward
in the same backing array (`w` never passes `r`, so nothing unread is
overwritten; the prefix before `head` needs no clearing, `w` writes over
it), `clear` on the tail beyond the new length so the GC can take those
tickets, then `tickets = tickets[:w]`, safe because the capacity stays
and later appends write over nil slots. Both ways end with `head = 0`,
`dead = 0`, and ticket pointers unchanged, so every ticket keeps its
own. The shrink check runs only inside a triggered compaction, never on
its own. Cost O(live) once per `live` removals, a few hundred
microseconds at 60k tickets, which lands on one caller's `Wait` return.
That is rare enough, and that is why
compaction is inline: a background goroutine would hold the same lock for
the same time and only add a lifecycle. `pending` is never compacted.
The thresholds are internal constants, not configuration. Not resliced
with `tickets[head:]`: that keeps the whole old array alive and does
nothing for holes.

## Construction, packages, logging

Packages. Root `github.com/go-dev-stack/stepline` holds `Limiter`, the
`Store` interface, `Period` (`PerSecond`, `PerMinute`), `CancelEvent`, the
errors. `store/redisstore` holds the Redis implementation and its Lua
files under `store/redisstore/lua/`, embedded with `//go:embed`. The
package is named `redisstore`, not `redis`, so it does not clash with
go-redis. `redisstore` imports the root; the root never imports
`redisstore`; root tests that need Redis live in the external package
`stepline_test`. A future store is another package under `store/`, and a
binary that imports it does not carry Redis.

Constructors fail loudly. Both take a context and return an error, and
both verify against Redis before returning, so a constructed Limiter can
really work, not nominally:

- `redisstore.NewStore(ctx, client, key, rate, period) (stepline.Store,
  error)`: loads the Lua scripts with `SCRIPT LOAD`. Proves the connection
  and the scripts; later calls use `EVALSHA` with no first-call fallback.
- `stepline.NewLimiter(ctx, store, opts...) (*Limiter, error)`: opens the
  subscription and waits for Redis to confirm it with `XLEN` on the
  stream, which returns 0 when the stream does not exist yet. Options:
  `WithLogger`.

After construction, Enqueue and Cancel take the usual path and return
whatever fails. The subscription is re-established by the client on
drops; what happens to events lost in a gap is [#3](https://github.com/go-dev-stack/stepline/issues/3).

Logging through `log/slog`, two levels. Debug for the normal flow: enqueue
reply, cancel sent, event applied, event skipped by version, backlog
replayed, compaction done. Warn for what the Subscriber sees and no caller
can, because there is no call to return it from: a reconnect, a malformed
event, a dropped subscription. Never Error: a store error on Enqueue or
Cancel goes back to the caller wrapped in the unavailable error, and the
caller decides. The default logger is `slog.New(slog.DiscardHandler)`, set
once in the constructor, so there is always a logger and no nil check.

## Chronology, ticket 17 on pod B, interval 1s

Queue 13, 14, 15, 16, head T+1.

| Time | Where | Event | 17's dueAt |
|---|---|---|---|
| T+0.2 | B -> Store | enqueue, position 4, slot T+5, reply [17, 4800000, 40] | T+5 |
| T+1.0 | A | 13 fires, no message | T+5 |
| T+2.0 | B | 14 fires, no message | T+5 |
| T+2.5 | C -> Store | cancel 15. Store trims 13, 14, head T+3, removes 15, event "cancelled 41 15" | T+4 |
| T+3.0 | A | 16 fires, no message | T+4 |
| T+3.2 | C -> Store | 18 enqueues. Store trims 16, head T+4, position 1, reply [18, 1800000, 41] | T+4 |
| T+4.0 | B | timer fires, Wait returns nil | done |

Idle: the last ticket fired at T+7.0, head is T+8.0. A newcomer at T+7.4
gets wait 600ms. A newcomer at T+9.3 finds the queue empty and head in the
past, head = now, wait 0.

## Deliberately absent

- No "may I go" request, no `permitted` event, no `ahead` in the Limiter.
- No 429 handling. The caller re-Waits or gives up (docs/429.md).
- No timestamps over the wire, no tuning knobs, no burst (v1.1+).

## Constraints of this design

Rules the happy path relies on. Breaking one breaks the schedule.

Time

- The Store never leaks its timestamps. Replies and events carry durations
  and counters only. A duration means the same on every pod; a timestamp
  does not, pod clocks and time zones differ.
- Redis `TIME` is the only clock for decisions. The Limiter uses its local
  monotonic clock only to sleep a duration it was given.
- A ticket never fires before its slot. It may fire a few milliseconds
  late; sub-millisecond precision is not promised. Lateness does not
  accumulate, because slots are counted from head, not from the previous fire.
- Time values are microseconds everywhere inside the Store. The interval
  is rounded up to a multiple of 100us and passed as a script argument on
  every call. The Store keeps rate and period and exposes the interval;
  the Limiter reads it once and keeps no copy of the config.

Queue

- One limiter key = one provider budget = one queue. The limiter must be
  the only caller of that budget.
- The design ceiling for the MVP is an interval of 5ms: 200 tickets per
  second, 12k per minute. It is the real expectation until benchmarks say
  otherwise; raising it is a later milestone. Every size and load figure
  in this document assumes at most that rate, and the 1k per second
  figures that remain are worst-case headroom, not a promise.
- Ticket numbers are never reused and never reset. Overflow is not a
  concern: `INCR` is a signed 64-bit integer and errors instead of
  wrapping, and the sorted set score stays exact up to 2^53. At the
  ceiling of 200 tickets per second the score bound is over a million
  years away.
- No per-ticket timestamps. A slot is always `head + position * interval`.
- Head is the slot of the first waiting ticket; it moves only by trim
  (forward) or by the idle rule (reset to now). Cancel never writes it.
- Trim happens only inside scripts. Redis has no timers. A ticket sends
  nothing when it fires.

Store

- Every operation is one Lua script: atomic, no interleaving.
- All keys of one limiter share a hash tag and the prefix `stepline:`.
- One event subscription per process, never per ticket.
- Every reply and every event carries the version. A Limiter applies only
  events above its reply's version.
- Events live in a stream that keeps at least the last 60 seconds. A
  reader continues from its last ID after a disconnect; only a disconnect
  longer than the retention can lose entries, and the version sequence
  shows it.

Limiter

- `Wait` has six outcomes: nil, the context error, the unavailable
  error, `ErrClosed` after `Shutdown`, `ErrDeadlineTooShort` when the
  queue position cannot fit the caller's deadline, `ErrNoDeadline` when
  the context has none. `ErrClosed` and `ErrNoDeadline` are programming
  errors that never happen in a correct integration. It
  never blocks on a Store error and never retries by itself.
- The queue has no fixed length bound. It is limited by the callers'
  deadlines (a ticket beyond `deadline / interval` positions is refused
  at enqueue), by the interval, and by memory: about 100 bytes per waiting
  number in Redis, about 150 bytes of registry per ticket in the pod plus
  the caller's own goroutine, which exists with or without the limiter.
- No 429 handling. The caller re-Waits or gives up.
- Cancel is sent on `context.WithoutCancel(ctx)`, best effort, bounded by
  the Redis client's own timeouts.
- `Shutdown(ctx)` is the only way down: graceful while `ctx` lives, one
  batched `Cancel` for the rest when it ends. No `Close`.
- The timer is reset only when dueAt changes.
- Config is rate and period, nothing else. No tuning knobs.
- Logging is `slog` Debug only; no logger means no output.
