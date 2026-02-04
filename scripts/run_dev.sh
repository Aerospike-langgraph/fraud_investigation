#!/bin/bash

# Development script for running backend and frontend with hot reload
# Infrastructure services run in Docker, app services run locally

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Fraud Detection - Development Mode   ${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

cd "$PROJECT_ROOT"

# Function to cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down...${NC}"
    
    # Kill background processes
    if [ ! -z "$BACKEND_PID" ]; then
        echo -e "${YELLOW}Stopping backend (PID: $BACKEND_PID)...${NC}"
        kill $BACKEND_PID 2>/dev/null || true
    fi
    
    if [ ! -z "$FRONTEND_PID" ]; then
        echo -e "${YELLOW}Stopping frontend (PID: $FRONTEND_PID)...${NC}"
        kill $FRONTEND_PID 2>/dev/null || true
    fi
    
    echo -e "${GREEN}Cleanup complete. Infrastructure containers still running.${NC}"
    echo -e "${YELLOW}To stop infrastructure: docker compose -f docker-compose.infra.yaml down${NC}"
    exit 0
}

trap cleanup SIGINT SIGTERM

# Step 1: Start infrastructure services
echo -e "${BLUE}[1/5] Starting infrastructure services...${NC}"
docker compose -f docker-compose.infra.yaml up -d

echo -e "${BLUE}[2/5] Waiting for services to be healthy...${NC}"
echo -n "Waiting for Aerospike DB"
until docker exec aerospike-db asinfo -p 3000 -v build &>/dev/null; do
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}✓${NC}"

echo -n "Waiting for Graph Service"
until curl -s http://localhost:9090/healthcheck &>/dev/null; do
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}✓${NC}"

echo -n "Waiting for Zipkin"
until curl -s http://localhost:9411/health &>/dev/null; do
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}✓${NC}"

echo ""

# Step 2: Setup Python virtual environment
echo -e "${BLUE}[3/5] Setting up Python backend...${NC}"
cd "$PROJECT_ROOT/backend"

# Find Python 3.10+ (required for match statements)
PYTHON_CMD=""
for py in python3.12 python3.11 python3.10; do
    if command -v $py &>/dev/null; then
        PYTHON_CMD=$py
        break
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    echo -e "${RED}Error: Python 3.10+ is required but not found.${NC}"
    echo "Please install Python 3.10 or newer: brew install python@3.11"
    exit 1
fi

echo "Using Python: $($PYTHON_CMD --version)"

# Check if venv exists and uses correct Python version
if [ -d "venv" ]; then
    VENV_PYTHON_VERSION=$(./venv/bin/python3 --version 2>/dev/null | cut -d' ' -f2 | cut -d'.' -f1,2)
    REQUIRED_VERSION=$($PYTHON_CMD --version | cut -d' ' -f2 | cut -d'.' -f1,2)
    
    if [ "$VENV_PYTHON_VERSION" != "$REQUIRED_VERSION" ]; then
        echo "Recreating venv with Python $REQUIRED_VERSION (was $VENV_PYTHON_VERSION)..."
        rm -rf venv
    fi
fi

if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    $PYTHON_CMD -m venv venv
fi

source venv/bin/activate
pip install -q -r requirements.txt

# Step 3: Start backend with hot reload
echo -e "${BLUE}[4/5] Starting backend with hot reload...${NC}"
export GRAPH_HOST_ADDRESS="localhost"
# Use --loop asyncio to avoid uvloop conflict with gremlin_python
uvicorn main:app --host 0.0.0.0 --port 4000 --reload --reload-dir . --loop asyncio &
BACKEND_PID=$!

# Wait for backend to be ready
echo -n "Waiting for backend"
until curl -s http://localhost:4000/health &>/dev/null; do
    echo -n "."
    sleep 1
done
echo -e " ${GREEN}✓${NC}"

# Step 4: Start frontend with hot reload
echo -e "${BLUE}[5/5] Starting frontend with hot reload...${NC}"
cd "$PROJECT_ROOT/frontend"

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo "Installing npm dependencies..."
    npm install
fi

npm run dev &
FRONTEND_PID=$!

# Wait for frontend to be ready
echo -n "Waiting for frontend"
sleep 3
until curl -s http://localhost:8080 &>/dev/null; do
    echo -n "."
    sleep 1
done
echo -e " ${GREEN}✓${NC}"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Development servers are running!     ${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "  ${BLUE}Frontend:${NC}  http://localhost:8080"
echo -e "  ${BLUE}Backend:${NC}   http://localhost:4000"
echo -e "  ${BLUE}Zipkin:${NC}    http://localhost:9411"
echo -e "  ${BLUE}Graph API:${NC} http://localhost:8182"
echo ""
echo -e "${YELLOW}Both backend and frontend have hot reload enabled.${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the dev servers.${NC}"
echo ""

# Keep script running and show logs
wait
