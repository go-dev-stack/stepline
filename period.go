package stepline

import (
	"fmt"
	"time"
)

type Period int

const (
	PerSecond Period = iota
	PerMinute
)

func (p Period) Duration() time.Duration {
	switch p {
	case PerSecond:
		return time.Second

	case PerMinute:
		return time.Minute

	default:
		panic(fmt.Sprintf("stepline: unknown period %d", int(p)))
	}
}
