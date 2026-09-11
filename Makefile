.PHONY: fmt check test build dev-up dev-smoke dev-traffic dev-down dev-logs

# `make check` is the go job from .github/workflows/ci.yml, runnable before a
# push. It did not exist, which is how two gofmt nits sat on main for eight
# commits. Note the $$: a single $ is make's own expansion, so `$(gofmt -l ...)`
# would expand to nothing here and pass unconditionally.
fmt:
	gofmt -w cmd internal
check:
	test -z "$$(gofmt -l cmd internal)"
	go vet ./...
	go test -race ./...

# Prefer the repository venv when it exists, so `make test` behaves the same
# whether or not it has been activated. README.md tells the reader to create one;
# without this, `make test` used whatever python was on PATH and failed on a
# missing pytest rather than on anything to do with the code.
PYTHON ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)

test:
	go test -race ./...
	$(PYTHON) -m pytest controlplane/tests -v
build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -o bin/gateway ./cmd/gateway

# Local end-to-end stack. See docs/LOCAL.md.
dev-up:
	./scripts/dev-up.sh
dev-smoke:
	./scripts/dev-smoke.sh
# Real traffic under several signed policies, ending with a simulated
# served-model change. Needs the stack from dev-up; see docs/LOCAL.md.
dev-traffic:
	./scripts/dev-traffic.sh
dev-down:
	./scripts/dev-down.sh
# Sources .dev/env for the same reason dev-up.sh does: compose interpolates
# ${APP_DB_PASSWORD} from the shell, not from env_file, so without this every
# invocation prints a "variable is not set" warning that reads like a fault.
dev-logs:
	@set -a; [ -f .dev/env ] && . ./.dev/env; set +a; \
	docker compose -f docker-compose.dev.yml logs -f gateway controlplane
