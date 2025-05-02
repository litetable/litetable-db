#include .env
SHELL := /bin/zsh
AIR := ${GOPATH}/bin/air

.PHONY: help
help: ## Display this help message
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort
	@echo ""

# Starts your local development server
.PHONY: local
local: ## Start the local development server
	${AIR} -c local/.air.toml

.PHONY: lint
lint: ## Run code linting
	@echo ""
	@echo "Running linter..."
	@echo ""
	./scripts/lint.sh

.PHONY: coverage
coverage: ## Run code coverage
	go tool cover -func cover.out

.PHONY: test
test: ## Run unit tests
	go test -v ./... -coverprofile cover.tmp
	grep -Ev "main.go|app|local|docs|_mock.go" cover.tmp > cover.out
	make coverage
	rm -rf cover.tmp

.PHONY: test-race
test-race: ## Run unit tests with race condition verification
	@echo ""
	@echo "Running unit tests with race condition checks."
	@echo ""
	go clean -testcache
	go test ./... -race
