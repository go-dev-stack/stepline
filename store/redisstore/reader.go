package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/go-dev-stack/stepline"
)

const (
	readBlockTimeout = time.Second
	readBatchCount   = 1000
	readRetryDelay   = 200 * time.Millisecond
)

func (s *redisStore) Subscribe(ctx context.Context) (<-chan *stepline.CancelEvent, error) {
	if err := s.probeEventsStream(ctx); err != nil {
		return nil, err
	}

	out := make(chan *stepline.CancelEvent)

	go s.readEvents(ctx, out)

	return out, nil
}

func (s *redisStore) probeEventsStream(ctx context.Context) error {
	if _, err := s.client.XLen(ctx, s.eventsKey).Result(); err != nil {
		return fmt.Errorf("redisstore: probe events stream: %w", err)
	}

	return nil
}

func (s *redisStore) readEvents(ctx context.Context, out chan<- *stepline.CancelEvent) {
	defer close(out)

	lastID := "$"

	for ctx.Err() == nil {
		res, err := s.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{s.eventsKey, lastID},
			Count:   readBatchCount,
			Block:   readBlockTimeout,
		}).Result()
		if err != nil {
			if !s.retryAfterReadError(ctx, err) {
				return
			}

			continue
		}

		next, ok := s.deliverBatch(ctx, out, res, lastID)

		if !ok {
			return
		}

		lastID = next
	}
}

func (s *redisStore) retryAfterReadError(ctx context.Context, err error) bool {
	if errors.Is(err, redis.Nil) {
		return true
	}

	if ctx.Err() != nil {
		return false
	}

	s.logger.WarnContext(ctx, "redisstore: xread failed", "error", err)

	select {
	case <-time.After(readRetryDelay):
		return true

	case <-ctx.Done():
		return false
	}
}

func (s *redisStore) deliverBatch(
	ctx context.Context,
	out chan<- *stepline.CancelEvent,
	streams []redis.XStream,
	lastID string,
) (string, bool) {
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			lastID = msg.ID

			ev, err := parseEvent(msg)
			if err != nil {
				s.logger.WarnContext(ctx, "redisstore: malformed event", "id", msg.ID, "error", err)

				continue
			}

			select {
			case out <- ev:
			case <-ctx.Done():
				return lastID, false
			}
		}
	}

	return lastID, true
}

func parseEvent(msg redis.XMessage) (*stepline.CancelEvent, error) {
	raw, ok := msg.Values["e"]

	if !ok {
		return nil, errors.New("redisstore: missing e field")
	}

	payload, ok := raw.(string)

	if !ok {
		return nil, fmt.Errorf("redisstore: e field not a string, got %T", raw)
	}

	fields := strings.Fields(payload)

	if len(fields) < 2 || fields[0] != "cancelled" {
		return nil, fmt.Errorf("redisstore: unexpected payload %q", payload)
	}

	version, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("redisstore: bad version in %q: %w", payload, err)
	}

	ticketFields := fields[2:]
	tickets := make([]int64, 0, len(ticketFields))

	for _, f := range ticketFields {
		n, ticketErr := strconv.ParseInt(f, 10, 64)
		if ticketErr != nil {
			return nil, fmt.Errorf("redisstore: bad ticket in %q: %w", payload, ticketErr)
		}

		tickets = append(tickets, n)
	}

	return &stepline.CancelEvent{Version: version, Tickets: tickets}, nil
}
