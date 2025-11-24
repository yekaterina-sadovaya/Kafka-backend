#!/bin/bash

set -e

echo "Setting up Kafka Backend..."

python_version=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')

required_major=3
required_minor=8

IFS='.' read -r major minor <<< "$python_version"

if (( major < required_major || (major == required_major && minor < required_minor) )); then
    echo "Error: Python 3.8+ required (found $python_version)"
    exit 1
fi

echo "Python $python_version"

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate

echo "Installing dependencies..."
pip install --upgrade pip > /dev/null
pip install -r requirements.txt

if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "Note: Review .env and update settings if needed"
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running"
    exit 1
fi

# Start Kafka
echo "Starting Kafka container..."
docker-compose up -d

echo "Waiting for Kafka to initialize..."
sleep 10

if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "Kafka is ready"
else
    echo "Warning: Kafka might need a few more seconds to start"
fi

echo ""
echo "Setup complete."
echo ""
echo "Next steps:"
echo "  source venv/bin/activate"
echo "  python producer.py"
echo "  python consumer.py    # in another terminal"
echo ""
echo "Other commands:"
echo "  python dlq_inspector.py summary"
echo "  python examples.py producer"
echo "  python monitoring.py"
echo ""
