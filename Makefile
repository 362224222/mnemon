# ──────────────────────────────────────────────────────────────────────
# Mnemon Makefile
# ──────────────────────────────────────────────────────────────────────

BINARY      := mnemon
VERSION     ?= dev
LDFLAGS     := -s -w -X github.com/mnemon-dev/mnemon/cmd.version=$(VERSION)
HARNESS_LDFLAGS := -s -w -X main.version=$(VERSION)
GO_VERSION   := $(shell awk '$$1 == "go" { print $$2; exit }' go.mod)
PINNED_GO    := env GOTOOLCHAIN=go$(GO_VERSION) GOFLAGS=-mod=readonly go
GOBIN       := $(shell go env GOBIN)
ifeq ($(GOBIN),)
  GOBIN     := $(shell go env GOPATH)/bin
endif

.PHONY: deps build harness-build install uninstall test unit vet harness-validate harness-quality harness-verify docker-build docker-run compose-up compose-down compose-dev release-snapshot clean help

.DEFAULT_GOAL := help

# ── Build ────────────────────────────────────────────────────────────

deps: ## Download Go dependencies
	go mod download

build: ## Build the mnemon binary
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) .

harness-build: ## Build the experimental R5 harness binaries
	go build -ldflags "$(HARNESS_LDFLAGS)" -o mnemon-harness ./harness/cmd/mnemon-harness
	go build -ldflags "$(HARNESS_LDFLAGS)" -o mnemond ./harness/cmd/mnemond

# ── Install / Uninstall ─────────────────────────────────────────────

install: build ## Build and install mnemon to $GOBIN
	@mkdir -p $(GOBIN)
	cp $(BINARY) $(GOBIN)/$(BINARY)
	@echo "Installed: $(GOBIN)/$(BINARY)"

uninstall: ## Remove mnemon binary from $GOBIN
	rm -f $(GOBIN)/$(BINARY)
	@echo "Removed: $(GOBIN)/$(BINARY)"
	@echo "Run 'mnemon setup --eject' first to remove integrations."

# ── Test ─────────────────────────────────────────────────────────────

test: build ## Run E2E test suite
	bash scripts/e2e_test.sh

unit: ## Run Go unit tests
	go test ./...

vet: ## Run go vet static analysis
	go vet ./...

harness-validate: ## Validate the experimental R5 harness layout
	bash harness/scripts/check_test_pairs.sh
	go test ./harness/internal/assets ./harness/internal/teamwork

harness-quality: ## Run pinned, non-mutating Harness quality gates
	@base_ref="$${HARNESS_QUALITY_BASE_REF:-HEAD}"; \
		$(PINNED_GO) run ./harness/tools/quality check --root . --base-ref "$$base_ref"
	$(PINNED_GO) vet ./harness/...
	$(PINNED_GO) test ./harness/tools/quality ./harness/internal/assets \
		./harness/internal/model ./harness/internal/event ./harness/internal/teamwork \
		./harness/test/contracts

harness-verify: ## Build and verify the experimental R5 Harness
	@set -eu; \
		tmp="$$(mktemp -d)"; \
		trap 'rm -rf "$$tmp"' EXIT; \
		$(PINNED_GO) build -o "$$tmp/mnemon" .; \
		$(PINNED_GO) build -o "$$tmp/mnemon-harness" ./harness/cmd/mnemon-harness; \
		$(PINNED_GO) build -o "$$tmp/mnemond" ./harness/cmd/mnemond
	bash harness/scripts/check_test_pairs.sh
	$(MAKE) harness-quality
	$(PINNED_GO) test ./harness/...

# ── Containers / Deployment ──────────────────────────────────────────

docker-build: ## Build runtime Docker image
	docker build --target runtime --build-arg VERSION=$(VERSION) -t mnemon-dev/mnemon:$(VERSION) .

docker-run: ## Run mnemon status in Docker with local .env
	docker run --rm --env-file .env -v mnemon-data:/data mnemon-dev/mnemon:$(VERSION) status

compose-up: ## Start mnemon with Docker Compose
	docker compose up -d mnemon

compose-down: ## Stop Docker Compose services
	docker compose down

compose-dev: ## Open a development shell in Docker Compose
	docker compose --profile dev run --rm mnemon-dev

release-snapshot: ## Build local GoReleaser snapshot artifacts
	goreleaser release --snapshot --clean

# ── Clean ────────────────────────────────────────────────────────────

clean: ## Remove build artifacts and test data
	rm -f $(BINARY) mnemon-harness mnemond
	rm -rf .testdata

# ── Help ─────────────────────────────────────────────────────────────

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
