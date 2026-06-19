#!/bin/bash

# API Server setup and run script

set -e

echo "=== Sandbox API Server Setup ==="

# Check Python
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is required but not installed"
    exit 1
fi
echo "Docker installed: $(docker --version)"

# Create virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cat > .env << EOF
# API Configuration
API_KEY=test-key-12345
DEBUG=false

# Server Configuration
HOST=0.0.0.0
PORT=8000

# Database Configuration
DATABASE_PATH=sandboxes.db

# Sandbox Configuration
DEFAULT_TEMPLATE=python:3.11
DEFAULT_CPU_LIMIT=1
DEFAULT_MEMORY_LIMIT=512m
DEFAULT_TIMEOUT=3600

# Logging
LOG_LEVEL=INFO
EOF
    echo ".env file created. Update with your configuration."
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To start the API server, run:"
echo "  source venv/bin/activate"
echo "  python main.py"
echo ""
echo "Or with uvicorn directly:"
echo "  uvicorn main:app --host 0.0.0.0 --port 8000"
echo ""
