#!/bin/bash

# Airflow Lab Management Script
# Usage: ./manage.sh [start|stop|logs]

show_usage() {
    echo "🔧 Airflow Lab Management Script"
    echo ""
    echo "Usage: ./manage.sh [command]"
    echo ""
    echo "Commands:"
    echo "  start   - Start Airflow services"
    echo "  stop    - Stop Airflow services"
    echo "  logs    - Show Airflow logs (follow mode)"
    echo ""
    echo "Examples:"
    echo "  ./manage.sh start"
    echo "  ./manage.sh stop"
    echo "  ./manage.sh logs"
    echo ""
}

start_airflow() {
    echo "🚀 Starting Airflow Lab..."
    echo ""

    # Create directories if they don't exist
    mkdir -p dags logs datasets

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
    echo "📊 Your DAGs should appear in a few minutes."
    echo ""
    echo "💡 Tip: Use './manage.sh logs' to monitor startup progress"
    echo ""
}

stop_airflow() {
    echo "🛑 Stopping Airflow Lab..."
    echo ""

    docker compose down

    echo ""
    echo "✅ Airflow services stopped."
    echo ""
    echo "💡 Tips:"
    echo "  - To restart: ./manage.sh start"
    echo "  - To completely reset (remove database): docker compose down -v"
    echo ""
}

show_logs() {
    echo "📋 Showing Airflow logs (press Ctrl+C to exit)..."
    echo ""

    # Check if services are running
    if ! docker compose ps | grep -q "Up"; then
        echo "⚠️  Airflow services are not running."
        echo "   Start them first with: ./manage.sh start"
        echo ""
        exit 1
    fi

    # Show logs with follow mode
    docker compose logs -f
}

# Main script logic
case "${1:-}" in
    start)
        start_airflow
        ;;
    stop)
        stop_airflow
        ;;
    logs)
        show_logs
        ;;
    "")
        echo "❌ No command specified."
        echo ""
        show_usage
        exit 1
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac
