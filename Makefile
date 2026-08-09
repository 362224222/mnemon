# ──────────────────────────────────────────────────────────────────────
# Mnemon Makefile
# ──────────────────────────────────────────────────────────────────────

BINARY      := mnemon
VERSION     ?= dev
LDFLAGS     := -s -w -X github.com/mnemon-dev/mnemon/cmd.version=$(VERSION)
HARNESS_LDFLAGS := -s -w -X main.version=$(VERSION)
GO_VERSION   := $(shell awk '$$1 == "go" { print $$2; exit }' go.mod)
HARNESS_GO_VERSION := $(shell awk '$$1 == "go" { print $$2; exit }' harness/go.mod)
HARNESS_GO   := env GOTOOLCHAIN=go$(HARNESS_GO_VERSION) GOFLAGS=-mod=readonly go -C harness
HARNESS_DETERMINISTIC_PKGS := \
	./cmd/mnemon-harness \
	./internal/agency \
	./internal/attach \
	./internal/authority \
	./internal/cas \
	./test/architecture \
	./test/observer \
	./test/r7/domainops/trace
HARNESS_TESTDATA_PKGS := \
	./testdata/r7/domain-ops/cmd/domain-load \
	./testdata/r7/domain-ops/cmd/domain-world \
	./testdata/r7/domain-ops/cmd/domainctl \
	./testdata/r7/domain-ops/world
GOBIN       := $(shell go env GOBIN)
ifeq ($(GOBIN),)
  GOBIN     := $(shell go env GOPATH)/bin
endif

.PHONY: deps build harness-build install uninstall test test-integration test-live
.PHONY: docker-build docker-run compose-up compose-down compose-dev release-snapshot clean help

.DEFAULT_GOAL := help

# ── Build ────────────────────────────────────────────────────────────

deps: ## Download Go dependencies
	go mod download
	$(HARNESS_GO) mod download

build: ## Build the mnemon binary
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) .

harness-build: ## Build the experimental R7 Harness binaries
	$(HARNESS_GO) build -ldflags "$(HARNESS_LDFLAGS)" -o ../mnemon-harness ./cmd/mnemon-harness
	$(HARNESS_GO) build -ldflags "$(HARNESS_LDFLAGS)" -o ../mnemond ./cmd/mnemond

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

test: ## Run deterministic tests without E2E, real daemon, or provider calls
	go vet ./...
	go test ./...
	$(HARNESS_GO) vet ./... $(HARNESS_TESTDATA_PKGS)
	$(HARNESS_GO) test $(HARNESS_DETERMINISTIC_PKGS) -count=1

test-integration: ## Run opt-in E2E, timing, race, process, and Docker tests
	bash scripts/e2e_test.sh
	$(HARNESS_GO) test -p 1 ./... $(HARNESS_TESTDATA_PKGS) -count=1
	$(HARNESS_GO) test -race -p 1 ./internal/... $(HARNESS_TESTDATA_PKGS) -count=1
	harness/test/r7/runner/run_cases.sh
	harness/test/r7/runtime/pi/run_delegate_oracle.sh
	harness/test/r7/domainops/run_world.sh

test-live: ## Run the paid Pi/DeepSeek smoke and federated operations case
	@test -n "$${DEEPSEEK_API_KEY:-}" || { echo "error: DEEPSEEK_API_KEY is required" >&2; exit 2; }
	LIVE_PI=1 harness/test/r7/runner/run_live_pi.sh
	LIVE_DOMAIN_OPS=1 harness/test/r7/domainops/run_live.sh

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
