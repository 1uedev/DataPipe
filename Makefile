GO_MODULES := proto/gen/go engine controlplane cli sdk tests

.PHONY: dev test itest bench lint proto build build-edge

dev:
	docker compose -f deploy/docker-compose.yml up --build -d
	cd ui && pnpm run dev

build:
	go build ./engine/... ./controlplane/... ./cli/... ./sdk/...

# build-edge cross-compiles the runtime as a static binary for edge devices
# (EDGE-110: "single small binary ... Linux x86-64 and ARM64"). CGO_ENABLED=0
# ensures no libc dependency, so the result runs unmodified on a minimal/
# musl-based edge image. Override EDGE_GOARCH=amd64 for x86-64 edge boxes.
EDGE_GOARCH ?= arm64
build-edge:
	cd engine && GOOS=linux GOARCH=$(EDGE_GOARCH) CGO_ENABLED=0 \
		go build -ldflags="-s -w" -o ../dist/datapipe-runtime-linux-$(EDGE_GOARCH) ./cmd/runtime
	@ls -lh dist/datapipe-runtime-linux-$(EDGE_GOARCH)

test:
	@for m in $(GO_MODULES); do (cd $$m && go test ./...) || exit 1; done
	cd ui && pnpm run test && pnpm run build

itest:
	cd tests && go test -tags itest ./...

bench:
	cd tests && go test -run '^$$' -bench . ./bench/...

lint:
	golangci-lint run ./engine/... ./controlplane/... ./cli/... ./sdk/... ./tests/...
	cd ui && pnpm run lint
	cd proto && buf lint

proto:
	cd proto && buf generate
