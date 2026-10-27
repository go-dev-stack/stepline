package stepline

import (
	"log/slog"
)

func runSubscriber(events <-chan *CancelEvent, reg *registry, logger *slog.Logger, done chan<- struct{}) {
	defer close(done)

	var lastVersion int64

	first := true

	for ev := range events {
		if !first && ev.Version != lastVersion+1 {
			logger.Warn("stepline: unexpected event version", "got", ev.Version, "want", lastVersion+1)
		}

		first = false
		lastVersion = ev.Version

		notified, skipped := reg.applyCancel(ev)
		logger.Debug(
			"stepline: event applied",
			"version", ev.Version,
			"tickets", len(ev.Tickets),
			"notified", notified,
			"skipped", skipped,
		)
	}
}
