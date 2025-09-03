#!/bin/bash

echo "🛑 Stopping Simple Airflow Lab..."

docker compose down

echo "✅ Airflow services stopped."
echo ""
echo "To completely reset (remove database):"
echo "  docker-compose down -v"
