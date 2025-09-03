#!/bin/bash

echo "🚀 Starting Airflow Lab Runtime Environment"
echo "==========================================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Creating .env file..."
    echo "AIRFLOW_UID=$(id -u)" > .env
    echo "_AIRFLOW_WWW_USER_USERNAME=airflow" >> .env
    echo "_AIRFLOW_WWW_USER_PASSWORD=airflow" >> .env
    echo "AIRFLOW_IMAGE_NAME=apache/airflow:3.0.6" >> .env
    echo "AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager" >> .env
    echo "AIRFLOW__CORE__EXPOSE_CONFIG=True" >> .env
    echo "AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True" >> .env
    echo "AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE=false" >> .env
    echo "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false" >> .env
    echo "AIRFLOW__LOGGING__LOGGING_LEVEL=INFO" >> .env
    echo "AIRFLOW__CORE__LOAD_EXAMPLES=false" >> .env
fi

# Create necessary directories
mkdir -p logs plugins

# Initialize Airflow (if needed)
echo "📊 Initializing Airflow database..."
docker compose up airflow-init

# Start all services
echo "🔧 Starting all Airflow services..."
docker compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check status
echo "📋 Service Status:"
docker compose ps

# Test connectivity
echo ""
echo "🌐 Testing Airflow UI connectivity..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/login | grep -q "200"; then
    echo "✅ Airflow UI is accessible at: http://localhost:8080"
    echo "   Username: airflow"
    echo "   Password: airflow"
else
    echo "⚠️  Airflow UI not yet ready. Check logs with: docker compose logs airflow-webserver"
fi

echo ""
echo "📈 Additional Services:"
echo "   - Elasticsearch: http://localhost:9200"
echo "   - Flower (Celery monitoring): Run 'docker compose --profile flower up' to enable"

echo ""
echo "🔧 Useful Commands:"
echo "   - View logs: docker compose logs [service-name]"
echo "   - Stop all: docker compose down"
echo "   - Restart service: docker compose restart [service-name]"
echo "   - Access container: docker compose exec [service-name] bash"
