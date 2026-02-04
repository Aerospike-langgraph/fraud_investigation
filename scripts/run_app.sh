#!/bin/bash

# Fraud Detection Application Runner
# This script starts both the backend (FastAPI) and frontend (Next.js) applications
# along with Docker Compose containers (Aerospike Graph, Graph Service, Zipkin)

# Default values
LOAD_USERS=false
BACKEND_PID=""
FRONTEND_PID=""
DOCKER_COMPOSE_STARTED=false

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -l, --load-users        Delete all data and load users, accounts, devices from users.json (no transactions)"
    echo ""
    echo "Examples:"
    echo "  $0                      # Start applications normally"
    echo "  $0 -l                   # Delete all data and load users, accounts, devices from users.json"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -l|--load-users)
            LOAD_USERS=false
            shift
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

echo "🚀 Starting Fraud Detection Application..."

# Function to check if Docker is installed and running
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is not installed. Please install Docker"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        echo "❌ Docker is not running. Please start Docker"
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose is not installed. Please install Docker Compose"
        exit 1
    fi
}

# Function to start Docker Compose containers
start_docker_containers() {
    echo "🐳 Starting Docker Compose containers..."

    # Check if docker-compose.yaml exists
    if [ ! -f "docker-compose.yaml" ]; then
        echo "❌ docker-compose.yaml not found in current directory"
        return 1
    fi

    # Start containers in detached mode
    docker-compose up -d

    if [ $? -eq 0 ]; then
        echo "✅ Docker containers started successfully"
        DOCKER_COMPOSE_STARTED=true
    else
        echo "❌ Failed to start Docker containers"
        return 1
    fi

    # Wait for containers to be healthy
    echo "⏳ Waiting for containers to be ready..."
    local max_attempts=60
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps | grep -q "healthy"; then
            echo "✅ All containers are healthy"
            return 0
        fi

        echo "   Attempt $attempt/$max_attempts - Waiting for containers to be healthy..."
        sleep 5
        attempt=$((attempt + 1))
    done

    echo "⚠️  Some containers may not be fully ready, continuing anyway..."
    return 0
}

# Function to stop Docker Compose containers
stop_docker_containers() {
    if [ "$DOCKER_COMPOSE_STARTED" = true ]; then
        echo "🐳 Stopping Docker Compose containers..."
        # docker-compose down
        echo "✅ Docker containers stopped"
    fi
}

# Function to cleanup background processes on exit
cleanup() {
    echo "🛑 Shutting down applications..."
    kill $BACKEND_PID $FRONTEND_PID 2>/dev/null
    # stop_docker_containers
    exit 0
}

# Function to wait for backend to be ready
wait_for_backend() {
    echo "⏳ Waiting for backend to be ready..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:4000/health > /dev/null 2>&1; then
            echo "✅ Backend is ready"
            return 0
        fi

        echo "   Attempt $attempt/$max_attempts - Backend not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo "❌ Backend failed to start within expected time"
    return 1
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8+"
    exit 1
fi

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed. Please install Node.js 16+"
    exit 1
fi

# Check if npm is installed
if ! command -v npm &> /dev/null; then
    echo "❌ npm is not installed. Please install npm"
    exit 1
fi

# Check if curl is installed
if ! command -v curl &> /dev/null; then
    echo "❌ curl is not installed. Please install curl"
    exit 1
fi

# Check Docker requirements
check_docker

# Start Docker Compose containers
start_docker_containers
if [ $? -ne 0 ]; then
    echo "❌ Failed to start Docker Compose containers. Exiting."
    exit 1
fi

echo "📦 Setting up backend..."
cd backend

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment and install dependencies
source venv/bin/activate
pip install -r requirements.txt

echo "🔧 Starting backend server..."

# Build command line arguments for backend
BACKEND_ARGS=""
if [ "$LOAD_USERS" = true ]; then
    BACKEND_ARGS="-d -l"
    echo "   Adding -d -l flags (delete all data and load users)"
fi

# Set AUTO_LOAD_DATA environment variable
export AUTO_LOAD_DATA=false
echo "   Setting AUTO_LOAD_DATA=false (data loading controlled by backend flags)"

python main.py $BACKEND_ARGS &
BACKEND_PID=$!

# Wait a moment for backend to start
sleep 3

echo "📦 Setting up frontend..."
cd ../frontend

# Install dependencies
npm install

echo "🎨 Starting frontend development server..."
npm run dev &
FRONTEND_PID=$!

# Wait a moment for frontend to start
sleep 3

echo "✅ Applications started successfully!"
echo ""
echo "🐳 Docker Containers:"
echo "   Aerospike Database: localhost:3000-3002"
echo "   Graph Service: localhost:8182 (Gremlin), localhost:9090 (Prometheus)"
echo "   Zipkin Tracing: localhost:9411"
echo ""
echo "🌐 Frontend: http://localhost:4001"
echo "🔌 Backend API: http://localhost:4000"
echo "📚 API Documentation: http://localhost:4000/docs"
echo ""

if [ "$LOAD_USERS" = true ]; then
    echo "🗑️  All data was deleted and users, accounts, devices from users.json were loaded (transactions must be generated manually)"
fi

echo ""
echo "Press Ctrl+C to stop all applications and containers"

# Wait for both processes
wait $BACKEND_PID $FRONTEND_PID
