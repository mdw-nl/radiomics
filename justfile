# Justfile for radiomics project

# Default recipe to display help
default:
    @just --list

# Start Docker Compose services
up:
    docker compose up -d --build

# Stop Docker Compose services
down:
    docker compose down

# Restart Docker Compose services
restart:
    docker compose restart

# View logs from all services
logs:
    docker compose logs -f

# Check status of all services
status:
    docker compose ps

# Install Python dependencies
install:
    uv sync

# Sync Python dependencies
sync:
    uv sync

# Run pre-commit hooks on all files
lint:
    uv run pre-commit run --all-files

# Clean up Docker volumes and rebuild from scratch
clean-build:
    docker compose down -v
    docker compose up -d --build

# Stash changes and checkout branch
checkout branch:
    git stash
    git checkout {{branch}}
