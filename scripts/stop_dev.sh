#!/bin/bash

# Stop all development services

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo -e "${YELLOW}Stopping all development services...${NC}"

# Kill any running uvicorn processes
echo "Stopping backend..."
pkill -f "uvicorn main:app" 2>/dev/null || true

# Kill any running next dev processes
echo "Stopping frontend..."
pkill -f "next dev" 2>/dev/null || true

# Stop infrastructure containers
echo "Stopping infrastructure containers..."
docker compose -f docker-compose.infra.yaml down 2>/dev/null || true

echo -e "${GREEN}All services stopped.${NC}"
