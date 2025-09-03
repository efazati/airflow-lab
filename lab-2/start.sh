#!/bin/bash

echo "🚀 Starting Simple Airflow Lab..."

# Set Airflow UID for file permissions
export AIRFLOW_UID=50000

# Create directories if they don't exist
mkdir -p dags logs

# The Docker Compose file now handles permissions via airflow-init-dirs service
echo "📁 Directories created. Permissions will be set by Docker..."

# Start Airflow services
echo "📦 Starting Docker services..."
docker compose up -d

echo ""
echo "✅ Airflow is starting up!"
echo ""
echo "🌐 Access Airflow at: http://localhost:8080"
echo "👤 Username: admin"
echo "🔐 Password: admin"
echo ""
echo "📊 Your simple DAG 'hello_world_dag' should appear in a few minutes."
echo ""
echo "To stop: docker-compose down"
echo "To reset: docker-compose down -v"
