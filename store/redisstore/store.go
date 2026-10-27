package redisstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/go-dev-stack/stepline"
)

const intervalRoundingUnit = 100 * time.Microsecond

type Option func(*config)

type config struct {
	logger *slog.Logger
}

func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

type redisStore struct {
	client   redis.UniversalClient
	interval time.Duration
	logger   *slog.Logger

	queueKey  string
	headKey   string
	seqKey    string
	verKey    string
	eventsKey string

	enqueueScript *redis.Script
	cancelScript  *redis.Script
}

func NewStore(
	ctx context.Context,
	client redis.UniversalClient,
	key string,
	rate int,
	period stepline.Period,
	opts ...Option,
) (stepline.Store, error) {
	if rate <= 0 {
		return nil, fmt.Errorf("redisstore: rate must be positive, got %d", rate)
	}

	if key == "" {
		return nil, errors.New("redisstore: key must not be empty")
	}

	cfg := config{logger: slog.New(slog.DiscardHandler)}

	for _, opt := range opts {
		opt(&cfg)
	}

	interval := roundInterval(period.Duration() / time.Duration(rate))

	s := &redisStore{
		client:    client,
		interval:  interval,
		logger:    cfg.logger,
		queueKey:  "stepline:{" + key + "}:queue",
		headKey:   "stepline:{" + key + "}:head",
		seqKey:    "stepline:{" + key + "}:seq",
		verKey:    "stepline:{" + key + "}:ver",
		eventsKey: "stepline:{" + key + "}:events",

		enqueueScript: redis.NewScript(trimLuaSource + enqueueLuaBody),
		cancelScript:  redis.NewScript(trimLuaSource + cancelLuaBody),
	}

	if err := s.enqueueScript.Load(ctx, client).Err(); err != nil {
		return nil, fmt.Errorf("redisstore: load enqueue script: %w", err)
	}

	if err := s.cancelScript.Load(ctx, client).Err(); err != nil {
		return nil, fmt.Errorf("redisstore: load cancel script: %w", err)
	}

	return s, nil
}

func (s *redisStore) Interval() time.Duration {
	return s.interval
}

func roundInterval(d time.Duration) time.Duration {
	rem := d % intervalRoundingUnit

	if rem == 0 {
		return d
	}

	return d - rem + intervalRoundingUnit
}

func toInt64(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil

	case int:
		return int64(t), nil

	default:
		return 0, fmt.Errorf("redisstore: unexpected numeric reply type %T", v)
	}
}

func (s *redisStore) Enqueue(ctx context.Context, budget time.Duration) (stepline.EnqueueResult, error) {
	budgetUs := max(budget.Microseconds(), 0)

	keys := []string{s.queueKey, s.headKey, s.seqKey, s.verKey}

	res, err := s.enqueueScript.Run(ctx, s.client, keys, s.interval.Microseconds(), budgetUs).Result()
	if err != nil {
		return stepline.EnqueueResult{}, err
	}

	values, ok := res.([]interface{})

	if !ok || len(values) != 3 {
		return stepline.EnqueueResult{}, fmt.Errorf("redisstore: unexpected enqueue reply %v", res)
	}

	ticket, err := toInt64(values[0])
	if err != nil {
		return stepline.EnqueueResult{}, err
	}

	waitUs, err := toInt64(values[1])
	if err != nil {
		return stepline.EnqueueResult{}, err
	}

	version, err := toInt64(values[2])
	if err != nil {
		return stepline.EnqueueResult{}, err
	}

	return stepline.EnqueueResult{
		Ticket:  ticket,
		Wait:    time.Duration(waitUs) * time.Microsecond,
		Version: version,
	}, nil
}

func (s *redisStore) Cancel(ctx context.Context, tickets ...int64) error {
	if len(tickets) == 0 {
		return nil
	}

	args := make([]interface{}, 0, len(tickets)+1)

	args = append(args, s.interval.Microseconds())

	for _, t := range tickets {
		args = append(args, t)
	}

	return s.cancelScript.Run(ctx, s.client, []string{s.queueKey, s.headKey, s.verKey, s.eventsKey}, args...).Err()
}
