# ──────────────────────────────────────────────────────────────────────
# Mnemon Makefile
# ──────────────────────────────────────────────────────────────────────

BINARY      := mnemon
VERSION     ?= dev
LDFLAGS     := -s -w -X github.com/mnemon-dev/mnemon/cmd.version=$(VERSION)
GOBIN       := $(shell go env GOBIN)
ifeq ($(GOBIN),)
  GOBIN     := $(shell go env GOPATH)/bin
endif

.PHONY: deps build harness-build install uninstall test unit vet harness-validate harness-docs-check eval-router-check codex-app-eval codex-app-eval-suite codex-memory-deep-eval codex-skill-deep-eval codex-eval-smoke docker-build docker-run compose-up compose-down compose-dev release-snapshot clean help

.DEFAULT_GOAL := help

# ── Build ────────────────────────────────────────────────────────────

deps: ## Download Go dependencies
	go mod download

build: ## Build the mnemon binary
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) .

harness-build: ## Build the harness binaries (mnemon-harness, mnemond, mnemonhub, Multica runtime adapter, and test-only acceptance runner)
	go build -ldflags "$(LDFLAGS)" -o mnemon-harness ./harness/cmd/mnemon-harness
	go build -ldflags "$(LDFLAGS)" -o mnemon-hub ./harness/cmd/mnemon-hub
	go build -ldflags "$(LDFLAGS)" -o mnemond ./harness/cmd/mnemond
	go build -ldflags "$(LDFLAGS)" -o mnemon-multica-runtime ./harness/cmd/mnemon-multica-runtime
	go build -ldflags "$(LDFLAGS)" -o mnemon-acceptance ./harness/cmd/mnemon-acceptance

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

harness-validate: ## Validate harness event packages
	bash scripts/validate_harness_loops.sh

harness-docs-check: ## Check bilingual harness doc heading sync
	bash scripts/check_bilingual_sync.sh

eval-router-check: ## Check no-model eval failed-finding routing to proposal
	bash scripts/check_eval_router_fixture.sh

codex-app-eval: ## Run real Codex app-server harness smoke eval
	python3 scripts/codex_app_server_eval.py

codex-app-eval-suite: ## Run real Codex app-server memory/skill scenario suite
	python3 scripts/codex_app_server_eval.py --suite

codex-memory-deep-eval: ## Run deep real Codex app-server memory regression suite
	python3 scripts/codex_app_server_eval.py --suite --suite-name memory-deep

codex-skill-deep-eval: ## Run deep real Codex app-server skill regression suite
	python3 scripts/codex_app_server_eval.py --suite --suite-name skill-deep

codex-eval-smoke: ## Run real Codex app-server eval projection smoke check
	python3 scripts/codex_app_server_eval.py --loop eval

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
	rm -f $(BINARY)
	rm -rf .testdata

# ── Help ─────────────────────────────────────────────────────────────

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
