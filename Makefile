# Mnemon build and verification entry points.

VERSION ?= dev
GOBIN   := $(shell go env GOBIN)
ifeq ($(GOBIN),)
  GOBIN := $(shell go env GOPATH)/bin
endif

BIN_DIR        := bin
MNEMON         := $(BIN_DIR)/mnemon
MNEMOND        := $(BIN_DIR)/mnemond
MNEMON_LDFLAGS := -s -w -X github.com/mnemon-dev/mnemon/internal/mnemoncli.version=$(VERSION)
MNEMOND_LDFLAGS := -s -w -X main.version=$(VERSION)

# Regular CI deliberately excludes real daemon readiness, process, TCP, Docker,
# JavaScript runtime, and paid-provider tests. Those belong to the explicit
# integration and live tiers below.
DETERMINISTIC_PKGS := \
	. \
	./cmd/mnemon \
	./cmd/mnemond \
	./internal/agency \
	./internal/attach \
	./internal/authority \
	./internal/cas \
	./internal/embed \
	./internal/graph \
	./internal/importdraft \
	./internal/mnemoncli \
	./internal/model \
	./internal/search \
	./internal/setup \
	./internal/setup/assets \
	./internal/store \
	./test/mnemond/architecture \
	./test/mnemond/observer \
	./test/mnemond/domainops/trace

TESTDATA_PKGS := \
	./testdata/mnemond/domainops/cmd/domain-load \
	./testdata/mnemond/domainops/cmd/domain-world \
	./testdata/mnemond/domainops/cmd/domainctl \
	./testdata/mnemond/domainops/world

.PHONY: deps build install uninstall test test-integration test-live
.PHONY: docker-build docker-run compose-up compose-down compose-dev release-snapshot clean help

.DEFAULT_GOAL := help

deps: ## Download Go dependencies
	go mod download

build: ## Build mnemon and mnemond
	@mkdir -p $(BIN_DIR)
	go build -ldflags "$(MNEMON_LDFLAGS)" -o $(MNEMON) ./cmd/mnemon
	go build -ldflags "$(MNEMOND_LDFLAGS)" -o $(MNEMOND) ./cmd/mnemond

install: build ## Install mnemon and mnemond to GOBIN
	@mkdir -p $(GOBIN)
	cp $(MNEMON) $(GOBIN)/mnemon
	cp $(MNEMOND) $(GOBIN)/mnemond
	@echo "Installed: $(GOBIN)/mnemon"
	@echo "Installed: $(GOBIN)/mnemond"

uninstall: ## Remove mnemon and mnemond from GOBIN
	rm -f $(GOBIN)/mnemon $(GOBIN)/mnemond
	@echo "Removed: $(GOBIN)/mnemon and $(GOBIN)/mnemond"
	@echo "Run 'mnemon setup --eject' first to remove Memory integrations."

test: ## Run the deterministic CI suite
	go vet ./...
	go test $(DETERMINISTIC_PKGS) -count=1

test-integration: ## Run opt-in E2E, process, network, race, runtime, and Docker tests
	bash scripts/e2e_test.sh
	go test -p 1 ./... $(TESTDATA_PKGS) -count=1
	go test -race -p 1 ./internal/... ./cmd/mnemond $(TESTDATA_PKGS) -count=1
	test/mnemond/scenarios/run_cases.sh
	test/mnemond/runtime/pi/run_delegate_oracle.sh
	test/mnemond/domainops/run_world.sh

test-live: ## Run the paid Pi/DeepSeek smoke and federated operations case
	@test -n "$${DEEPSEEK_API_KEY:-}" || { echo "error: DEEPSEEK_API_KEY is required" >&2; exit 2; }
	LIVE_PI=1 test/mnemond/scenarios/run_live_pi.sh
	LIVE_DOMAIN_OPS=1 test/mnemond/domainops/run_live.sh

docker-build: ## Build the runtime Docker image
	docker build --target runtime --build-arg VERSION=$(VERSION) -t mnemon-dev/mnemon:$(VERSION) .

docker-run: ## Run mnemon status in Docker with local .env
	docker run --rm --env-file .env -v mnemon-data:/mnemon mnemon-dev/mnemon:$(VERSION) status

compose-up: ## Start the Mnemon container
	docker compose up -d mnemon

compose-down: ## Stop Docker Compose services
	docker compose down

compose-dev: ## Open a development shell in Docker Compose
	docker compose --profile dev run --rm mnemon-dev

release-snapshot: ## Build local GoReleaser snapshot artifacts
	goreleaser release --snapshot --clean

clean: ## Remove build artifacts and test data
	rm -f mnemon mnemond
	rm -f $(BIN_DIR)/mnemon $(BIN_DIR)/mnemond
	rm -rf .testdata

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
