GO ?= go
GO_TOOLCHAIN ?= $(shell awk '$$1 == "toolchain" { print $$2; exit }' go.mod)
STATICCHECK_VERSION ?= 2026.1
GOVULNCHECK_VERSION ?= v1.6.0
DOCKER ?= docker
IMAGE ?= bloar:dev
VCS_REF ?= $(shell git rev-parse --verify HEAD 2>/dev/null)
BUILD_DATE ?= $(shell git show -s --format=%cI HEAD 2>/dev/null)
SOURCE_URL ?= https://github.com/blobarchive/bloar
MODULE_PATH ?= github.com/blobarchive/bloar

.PHONY: build test lint conformance dependency-check vulncheck p2p-smoke \
	supply-chain-check docker docker-edge

build:
	$(GO) build ./...

test:
	$(GO) test ./...

lint:
	@test -n "$(GO_TOOLCHAIN)" || { \
		echo "go.mod must declare the exact toolchain used by lint"; \
		exit 1; \
	}
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt'd:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO) vet ./...
	GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...

# The conformance suite (spec 13.1) is a separate module: it imports nitro,
# whose dependency graph -- and whose replace directives pinning a go-ethereum
# fork -- must not reach the root module. The targets above therefore do not
# see it, and this one is not part of `test`: it is a heavier build, and CI runs
# it as its own job.
conformance:
	cd conformance && $(GO) test ./...

dependency-check:
	@test "$$($(GO) list -m -f '{{.Version}}' github.com/quic-go/quic-go)" = "v0.60.0"
	@cd conformance && \
		test "$$($(GO) list -m -f '{{.Version}}' github.com/quic-go/quic-go)" = "v0.60.0"

vulncheck:
	./scripts/govulncheck-gate.sh $(GOVULNCHECK_VERSION)
	cd conformance && ../scripts/govulncheck-gate.sh $(GOVULNCHECK_VERSION)

p2p-smoke:
	$(GO) test ./p2p

supply-chain-check:
	./scripts/verify-supply-chain-policy.sh

# The deployment image (deploy/Dockerfile): both binaries, CGO_ENABLED=0, into
# distroless. Not part of any other target and not in CI -- it needs a daemon,
# and nothing above it does. See docs/operations.md.
#
# The context is the repository root rather than deploy/, because the build
# stage needs the source.
docker:
	@test -n "$(VCS_REF)" || { \
		echo "cannot determine VCS revision; build from a Git checkout or set VCS_REF explicitly"; \
		exit 1; \
	}
	@test -n "$(BUILD_DATE)" || { echo "BUILD_DATE must not be empty"; exit 1; }
	@test -n "$(SOURCE_URL)" || { echo "SOURCE_URL must not be empty"; exit 1; }
	@test -n "$(MODULE_PATH)" || { echo "MODULE_PATH must not be empty"; exit 1; }
	@test "$$(git rev-parse --verify HEAD)" = "$(VCS_REF)" || { \
		echo "VCS_REF must equal the checked-out HEAD"; \
		exit 1; \
	}
	@test -z "$$(git status --porcelain --untracked-files=all)" || { \
		echo "refusing to build an image from a dirty or untracked worktree"; \
		git status --short; \
		exit 1; \
	}
	@build_root=$$(mktemp -d); \
	trap 'rm -rf -- "$$build_root"' EXIT HUP INT TERM; \
	./scripts/create-build-context.sh "$$build_root/context" "$(VCS_REF)"; \
	./scripts/create-build-vcs-snapshot.sh "$$build_root/vcs-snapshot.tar" "$(VCS_REF)"; \
	$(DOCKER) build \
		--no-cache \
		--secret id=vcs_snapshot,src="$$build_root/vcs-snapshot.tar" \
		--build-arg VCS_REF="$(VCS_REF)" \
		--build-arg BUILD_DATE="$(BUILD_DATE)" \
		--build-arg SOURCE_URL="$(SOURCE_URL)" \
		--build-arg MODULE_PATH="$(MODULE_PATH)" \
		-f deploy/Dockerfile \
		-t $(IMAGE) \
		"$$build_root/context"

# The public edge intentionally uses a distinct runtime target with no VOLUME
# declarations; see deploy/Dockerfile.
docker-edge:
	@test -n "$(VCS_REF)" || { \
		echo "cannot determine VCS revision; build from a Git checkout or set VCS_REF explicitly"; \
		exit 1; \
	}
	@test -n "$(BUILD_DATE)" || { echo "BUILD_DATE must not be empty"; exit 1; }
	@test -n "$(SOURCE_URL)" || { echo "SOURCE_URL must not be empty"; exit 1; }
	@test -n "$(MODULE_PATH)" || { echo "MODULE_PATH must not be empty"; exit 1; }
	@test "$$(git rev-parse --verify HEAD)" = "$(VCS_REF)" || { \
		echo "VCS_REF must equal the checked-out HEAD"; \
		exit 1; \
	}
	@test -z "$$(git status --porcelain --untracked-files=all)" || { \
		echo "refusing to build an image from a dirty or untracked worktree"; \
		git status --short; \
		exit 1; \
	}
	@build_root=$$(mktemp -d); \
	trap 'rm -rf -- "$$build_root"' EXIT HUP INT TERM; \
	./scripts/create-build-context.sh "$$build_root/context" "$(VCS_REF)"; \
	./scripts/create-build-vcs-snapshot.sh "$$build_root/vcs-snapshot.tar" "$(VCS_REF)"; \
	$(DOCKER) build \
		--no-cache \
		--secret id=vcs_snapshot,src="$$build_root/vcs-snapshot.tar" \
		--build-arg VCS_REF="$(VCS_REF)" \
		--build-arg BUILD_DATE="$(BUILD_DATE)" \
		--build-arg SOURCE_URL="$(SOURCE_URL)" \
		--build-arg MODULE_PATH="$(MODULE_PATH)" \
		--target edge \
		-f deploy/Dockerfile \
		-t $(IMAGE) \
		"$$build_root/context"
