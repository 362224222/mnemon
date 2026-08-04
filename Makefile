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
GOBIN       := $(shell go env GOBIN)
ifeq ($(GOBIN),)
  GOBIN     := $(shell go env GOPATH)/bin
endif

.PHONY: deps build harness-build install uninstall test unit vet harness-validate harness-quality harness-verify
.PHONY: harness-contract harness-static harness-docker harness-docker-case harness-live-pi
.PHONY: harness-r8 harness-r8-docker harness-domain-ops
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

test: build ## Run E2E test suite
	bash scripts/e2e_test.sh

unit: ## Run Go unit tests
	go test ./...

vet: ## Run go vet static analysis
	go vet ./...

harness-validate: ## Validate the R7 projection, contract, and evidence bindings
	$(HARNESS_GO) test ./internal/attach ./tools/corecontract ./test/contracts -count=1

harness-quality: ## Run pinned, non-mutating Harness quality gates
	@base_ref="$${HARNESS_QUALITY_BASE_REF:-HEAD}"; \
		$(HARNESS_GO) run ./tools/quality check --root .. --base-ref "$$base_ref"
	$(HARNESS_GO) vet ./...
	$(HARNESS_GO) test ./tools/quality ./tools/corecontract ./test/contracts -count=1

harness-contract: ## Validate the active R7 contract and evidence registry
	$(HARNESS_GO) test ./tools/corecontract ./test/contracts -count=1

harness-static: ## Run R7 pattern, layout, and deletion oracles
	harness/test/r7/runner/run_static.sh

harness-docker: ## Run all R7 cases in isolated Docker peers
	harness/test/r7/runner/run_cases.sh

harness-docker-case: ## Run one R7 Docker case with CASE=<name>
	@test -n "$(CASE)" || { echo "error: CASE is required" >&2; exit 2; }
	harness/test/r7/runner/run_cases.sh "$(CASE)"

harness-live-pi: ## Run the opt-in Pi/DeepSeek live smoke
	@test "$${LIVE_PI:-}" = 1 || { echo "error: set LIVE_PI=1" >&2; exit 2; }
	@test -n "$${DEEPSEEK_API_KEY:-}" || { echo "error: DEEPSEEK_API_KEY is required" >&2; exit 2; }
	harness/test/r7/runner/run_live_pi.sh

harness-r8: ## Test the optional, removable R8 selector and its proof adapters
	$(HARNESS_GO) test ./internal/selector ./internal/selector/simtest ./internal/selector/testdata/network/cmd/r8-peer -count=1
	$(HARNESS_GO) test -race ./internal/selector ./internal/selector/simtest ./internal/selector/testdata/network/cmd/r8-peer -count=1

harness-r8-docker: harness-r8 ## Run the isolated five-peer R8 network proof
	harness/test/r8/network/runner/run_docker.sh

harness-domain-ops: ## Run the opt-in real-service federated operations world
	harness/test/r7/domainops/run_world.sh

harness-verify: harness-quality ## Run the complete exact-tree R7 merge gate and write its report
	$(HARNESS_GO) run ./tools/corecontract/cmd/core-gate --root ..

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
