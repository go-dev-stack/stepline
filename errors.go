package stepline

import "errors"

var (
	ErrStoreUnavailable = errors.New("stepline: store unavailable")
	ErrClosed           = errors.New("stepline: limiter closed")
	ErrDeadlineTooShort = errors.New("stepline: deadline too short")
	ErrNoDeadline       = errors.New("stepline: context has no deadline")
)
