GO_MODULES := proto/gen/go engine controlplane cli sdk

.PHONY: dev test itest bench lint proto build

dev:
	docker compose -f deploy/docker-compose.yml up --build -d
	cd ui && pnpm run dev

build:
	go build ./engine/... ./controlplane/... ./cli/... ./sdk/...

test:
	@for m in $(GO_MODULES); do (cd $$m && go test ./...) || exit 1; done
	cd ui && pnpm run build

itest:
	go test -tags itest ./tests/...

bench:
	go test -run '^$$' -bench . ./tests/bench/...

lint:
	golangci-lint run ./engine/... ./controlplane/... ./cli/... ./sdk/...
	cd ui && pnpm run lint
	cd proto && buf lint

proto:
	cd proto && buf generate
