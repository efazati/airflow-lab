#!/bin/bash

echo "🔍 Airflow Lab Status Check"
echo "=========================="
echo ""

# Check Docker Compose services
echo "📋 Service Status:"
docker compose ps
echo ""

# Check DAG loading
echo "🔧 Testing DAG Loading:"
docker compose exec airflow-scheduler python /opt/airflow/dags/all_dags.py 2>&1 | grep -E "(Loaded|Error)" | head -10
echo ""

# Check if DAGs are recognized
echo "📊 Available DAGs:"
docker compose exec airflow-scheduler airflow dags list 2>/dev/null | head -10
echo ""

# Test UI connectivity
echo "🌐 UI Connectivity Test:"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/login 2>/dev/null)
if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ Airflow UI is accessible at: http://localhost:8080"
    echo "   Username: airflow"
    echo "   Password: airflow"
elif [ "$HTTP_CODE" = "000" ]; then
    echo "❌ Airflow UI is not accessible (connection refused)"
    echo "   Check if webserver is running: docker compose logs airflow-webserver"
else
    echo "⚠️  Airflow UI returned HTTP $HTTP_CODE"
    echo "   Still starting up or configuration issue"
fi
echo ""

# Check recent logs for errors
echo "🔍 Recent Errors (if any):"
docker compose logs --tail 20 2>&1 | grep -i error | tail -5
echo ""

# Provide next steps
echo "📝 Next Steps:"
echo "1. Access Airflow UI: http://localhost:8080 (airflow/airflow)"
echo "2. Check DAG visibility in the UI"
echo "3. If DAGs don't show code, check the troubleshooting guide"
echo "4. View logs: docker compose logs [service-name]"
echo ""
echo "🔧 Quick Commands:"
echo "- Restart services: docker compose restart"
echo "- Stop all: docker compose down"
echo "- View specific logs: docker compose logs airflow-webserver --tail 50"
