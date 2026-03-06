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

# Install dev + test dependencies (includes pyradiomics and platipy)
install-test:
    uv sync --group test

# Run integration tests against real DICOM data
# Usage: just integration-test /path/to/dicom
#        just integration-test /path/to/dicom /path/to/output
integration-test dicom_folder output_dir="":
    #!/usr/bin/env bash
    set -euo pipefail
    extra=""
    if [ -n "{{output_dir}}" ]; then extra="--output-dir {{output_dir}}"; fi
    uv run --group test pytest tests/integration/ -v -s \
        --dicom-folder "{{dicom_folder}}" $extra

# Run standalone pipeline script without pytest
# Usage: just run-pipeline /path/to/dicom
#        just run-pipeline /path/to/dicom ./results
run-pipeline dicom_folder output_dir="./radiomics_results":
    uv run --group test python run_integration.py \
        --dicom-folder "{{dicom_folder}}" \
        --output-dir "{{output_dir}}"

# Stash changes and checkout branch
checkout branch:
    git stash
    git checkout {{branch}}
