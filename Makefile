.PHONY: dev dev-backend dev-frontend build run test lint clean tools

# Default target
all: build

# Run development mode (Hot reloading for Go and Next.js via mprocs)
dev: tools
	@echo "Starting development environment with mprocs..."
	mprocs

# Install dev tools if missing
tools:
	@command -v air >/dev/null 2>&1 || (echo "Installing air..." && go install github.com/air-verse/air@v1.52.2)
	@command -v mprocs >/dev/null 2>&1 || (echo "Installing mprocs..." && brew install mprocs)
	@command -v golangci-lint >/dev/null 2>&1 || (echo "Installing golangci-lint..." && brew install golangci-lint)

# Build for production
build:
	@echo "Building UI..."
	cd web && npm run build
	@echo "Building Go binary..."
	go build -o nagare .

# Run production binary
run: build
	./nagare

# Run Go tests
test:
	CGO_ENABLED=1 go test -race ./...

# Run all linters (Go + frontend)
lint:
	@echo "Running Go linter..."
	CGO_ENABLED=1 golangci-lint run ./...
	@echo "Running frontend linter..."
	cd web && npm run lint
	@echo "Running frontend type check..."
	cd web && npx tsc --noEmit

# Clean build artifacts
clean:
	rm -rf nagare web/out web/.next tmp/
