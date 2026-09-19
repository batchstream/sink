.PHONY: proto build test test-unit test-integration test-search-integration fmt check-format lint lint-workflows lint-docs quickstart quickstart-down test-isolated-quickstart

PROTO_DIR := proto
GEN_DIR := gen
STATICCHECK_VERSION := v0.8.1
ACTIONLINT_VERSION := v1.7.12
LYCHEE ?= lychee

build:
	go build -o bin/sink ./cmd/sink

proto:
	@mkdir -p $(GEN_DIR)
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		--go-vtproto_out=$(GEN_DIR) --go-vtproto_opt=paths=source_relative,features=marshal+unmarshal+size+pool \
		$(PROTO_DIR)/sink/sink.proto $(PROTO_DIR)/forward/forward.proto
	@printf '%s\n%s\n' '// Package sink contains generated protobuf definitions for the Sink gRPC service.' 'package sink' > $(GEN_DIR)/sink/doc.go
	@printf '%s\n%s\n' '// Package forward contains the private Gateway-to-Engine protobuf contract.' 'package forward' > $(GEN_DIR)/forward/doc.go

test:
	go test ./... -v -count=1

test-unit:
	go test ./internal/... -v -count=1

test-integration:
	bash scripts/test-mongodb-integration.sh
	bash scripts/test-search-integration.sh elasticsearch
	bash scripts/test-search-integration.sh opensearch

test-search-integration:
	bash scripts/test-search-integration.sh elasticsearch
	bash scripts/test-search-integration.sh opensearch

test-isolated-quickstart:
	bash scripts/test-isolated-quickstart.sh

quickstart:
	bash examples/quickstart/run.sh

quickstart-down:
	docker compose --file examples/quickstart/compose.yaml down

fmt:
	gofmt -s -w .

check-format:
	@set -e; files="$$(gofmt -l .)"; \
	if [ -n "$$files" ]; then \
		printf '%s\n' "$$files"; \
		printf '%s\n' 'Run make fmt to format these files.'; \
		exit 1; \
	fi

lint: check-format
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -checks=all ./...

lint-workflows:
	go run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION) -shellcheck=''

lint-docs:
	$(LYCHEE) --offline --include-fragments --no-progress '*.md' 'configs/*.md' 'docs/**/*.md' 'examples/**/*.md' 'benchmarks/**/*.md' '.github/*.md'

# Core-package floors are kept separately from generated code and examples.
.PHONY: test-coverage
COVERAGE_DIR ?= .reports/coverage
test-coverage:
	@mkdir -p $(COVERAGE_DIR)
	go test -mod=readonly -race -covermode=atomic -coverpkg=./... -coverprofile=$(COVERAGE_DIR)/unit.out -count=1 -timeout=10m -json ./... > $(COVERAGE_DIR)/unit.jsonl
	python3 -m unittest discover -s scripts -p 'test_coverage.py'
	python3 scripts/check-coverage.py --profile $(COVERAGE_DIR)/unit.out --minimums .github/coverage-minimums.json --report $(COVERAGE_DIR)/summary.md
