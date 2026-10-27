GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
GOTESTSUM := go run gotest.tools/gotestsum@latest
GOTESTSUM_FORMAT ?= pkgname

.PHONY: lint lint-fix test
lint:
	$(GOLANGCI_LINT) run ./...

lint-fix:
	$(GOLANGCI_LINT) run --fix ./...

test:
	$(GOTESTSUM) --hide-summary=skipped --format $(GOTESTSUM_FORMAT) -- -race ./...
