.PHONY: proto build test vet tidy clean dev-up dev-down

GO ?= go
BUF ?= buf

MODULES := api pkg counter match bff push quote trade-dump trigger history admin-gateway asset

# ---------------------------------------------------------------------------
# Proto generation (buf + Connect Go remote plugins)
# ---------------------------------------------------------------------------

PROTO_GO_OUT := api/gen

proto: ## Generate Go code from .proto files via buf (protobuf-go + connectrpc/go)
	$(BUF) generate
	@echo "proto generated into $(PROTO_GO_OUT)"

# ---------------------------------------------------------------------------
# Build / test
# ---------------------------------------------------------------------------

build: ## Build all modules
	@for m in $(MODULES); do \
		echo ">> build $$m"; \
		(cd $$m && $(GO) build ./...) || exit 1; \
	done

test: ## Run all tests
	@for m in $(MODULES); do \
		echo ">> test $$m"; \
		(cd $$m && $(GO) test ./...) || exit 1; \
	done

vet: ## Run go vet
	@for m in $(MODULES); do \
		(cd $$m && $(GO) vet ./...) || exit 1; \
	done

tidy: ## go mod tidy each module
	@for m in $(MODULES); do \
		echo ">> tidy $$m"; \
		(cd $$m && $(GO) mod tidy) || exit 1; \
	done

# ---------------------------------------------------------------------------
# Dev environment (docker compose)
# ---------------------------------------------------------------------------

COMPOSE := docker compose -f deploy/docker/docker-compose.yml

dev-up: ## Start local dependencies (kafka, etcd, mysql, minio)
	$(COMPOSE) up -d

dev-down: ## Stop local dependencies
	$(COMPOSE) down

dev-logs: ## Tail logs from local deps
	$(COMPOSE) logs -f

# ---------------------------------------------------------------------------

clean:
	rm -rf $(PROTO_GO_OUT)
	@for m in $(MODULES); do (cd $$m && $(GO) clean ./...) ; done

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
