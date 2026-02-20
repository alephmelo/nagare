.PHONY: dev dev-backend dev-frontend build run clean tools

# Default target
all: build

# Run development mode (Hot reloading for Go and Next.js via mprocs)
dev: tools
	@echo "Starting development environment with mprocs..."
	mprocs

# Install dev tools if missing
tools:
	@command -v air >/dev/null 2>&1 || (echo "Installing air..." && go install github.com/air-verse/air@latest)
	@command -v mprocs >/dev/null 2>&1 || (echo "Installing mprocs..." && brew install mprocs)

# Build for production
build:
	@echo "Building UI..."
	cd web && npm run build
	@echo "Building Go binary..."
	go build -o nagare .

# Run production binary
run: build
	./nagare

# Clean build artifacts
clean:
	rm -rf nagare web/out web/.next tmp/
