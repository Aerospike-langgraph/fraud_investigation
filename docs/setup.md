# Setup Instructions

This guide will help you set up the Fraud Detection Application on your local machine.

## 📋 Prerequisites

### Required Software
1. **Docker & Docker Compose** - For Aerospike Graph Service and related services
   - [Install Docker Desktop](https://docs.docker.com/get-docker/) (includes Docker Compose)
   - Ensure Docker is running before starting the application
2. **Python 3.8+** - Backend runtime
3. **Node.js 16+** - Frontend runtime
4. **Git** - Version control

### System Requirements
- **RAM**: 4GB minimum, 8GB recommended
- **Storage**: 2GB free space for Docker images
- **OS**: Windows 10+, macOS 10.15+, or Linux (Ubuntu 18.04+)

## 🐳 Containerized Services

The application uses Docker Compose to automatically provide:
- **Aerospike Database Server** - Graph data storage (ports 3000-3002)
- **Aerospike Graph Service (AGS)** - Graph query engine (port 8182)
- **Zipkin** - Distributed tracing (port 9411)



## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone <repository-url>
cd DemoApp
```

### 2. Run the Application (Recommended)
The `run_app.sh` script handles everything automatically:

```bash
# Basic startup (starts Docker services + backend + frontend)
./run_app.sh

# Show all available options
./run_app.sh --help

# Load sample data from users.json
./run_app.sh -l
```

**Command Line Options:**
- `-h, --help` - Show help message
- `-l, --load-users` - Delete all data and load users, accounts, devices from users.json

The script will automatically:
1. **Check Docker availability** and start Docker Compose services
2. **Wait for services to be healthy** (Aerospike DB, Graph Service, Zipkin)
3. **Create Python virtual environment** and install dependencies
4. **Install Node.js dependencies**
5. **Start the FastAPI backend** on port 4000
6. **Start the Next.js frontend** on port 4001
7. **Optionally load sample data**

### 3. Access the Application
Once setup is complete, you can access:
- **Frontend**: http://localhost:4001
- **Backend API**: http://localhost:4000
- **API Documentation**: http://localhost:4000/docs
- **Zipkin Tracing**: http://localhost:9411 (optional)

## 🛠️ Manual Setup (Advanced)

If you prefer to run services manually:

### 1. Start Docker Services
```bash
# Start containerized services
docker-compose up -d

# Check service health
docker-compose ps

# Wait for services to be ready (look for "healthy" status)
```

### 2. Backend Setup

```bash
cd backend

# Create and activate virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt

# Start the backend server
python main.py
```
The backend will be available at http://localhost:4000

### 3. Frontend Setup

```bash
cd frontend

# Install Node.js dependencies
npm install

# Start the development server
npm run dev
```
The frontend will be available at http://localhost:3000 (or port 4001 with custom config)

## 📊 Sample Data Setup

### Loading Sample Data
The application includes sample data in `data/users.json`. To load this data:

1. **Using the API endpoint**
   ```bash
   curl -X POST http://localhost:4000/seed-data
   ```

2. **Using the run script**
   ```bash
   ./run_app.sh --load-sample
   ```

### Sample Data Structure
The sample data includes:
- **Users**: 100+ sample users with realistic profiles
- **Accounts**: Multiple accounts per user (savings, checking, etc.)
- **Devices**: User devices with fingerprints and login history
- **Fraud Flags**: Some accounts and devices are pre-flagged for testing


### Environment Variables
Create a `.env` file in the backend directory for custom configuration:

```env
# Backend Configuration
HOST=localhost
PORT=4000
DEBUG=true

# Graph Service Configuration (automatically provided by Docker)
GRAPH_HOST=localhost
GRAPH_PORT=8182

# Frontend Configuration
NEXT_PUBLIC_API_URL=http://localhost:4000
```

