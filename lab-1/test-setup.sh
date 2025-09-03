#!/bin/bash

echo "🧪 Testing Airflow Lab DAG Factory Setup"
echo "========================================"
echo ""

# Test DAG Factory loading
echo "📋 Testing DAG Factory Loading:"
docker compose exec airflow-scheduler python /opt/airflow/dags/dag_factory_loader.py
echo ""

# Test DAG discovery
echo "🔍 Testing DAG Discovery:"
docker compose exec airflow-scheduler airflow dags list 2>/dev/null || echo "DAGs not yet discovered by scheduler"
echo ""

# Test import errors
echo "❌ Checking Import Errors:"
docker compose exec airflow-scheduler airflow dags list-import-errors 2>/dev/null || echo "No import errors command available"
echo ""

# Test UI access
echo "🌐 Testing UI Access:"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/ 2>/dev/null)
if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ Web UI accessible at: http://localhost:8080"
    echo "   Username: airflow"
    echo "   Password: airflow"
else
    echo "❌ Web UI not accessible (HTTP $HTTP_CODE)"
fi
echo ""

# Test API access
echo "🔌 Testing API Access:"
API_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v2/dags 2>/dev/null)
if [ "$API_CODE" = "401" ]; then
    echo "✅ API server responding (authentication required)"
else
    echo "❌ API server not responding correctly (HTTP $API_CODE)"
fi
echo ""

echo "📊 Service Status:"
docker compose ps --format table
echo ""

echo "💡 Next Steps:"
echo "1. Open http://localhost:8080 in your browser"
echo "2. Login with username: airflow, password: airflow"
echo "3. Check if your YAML-defined DAGs are visible in the UI"
echo "4. For code visibility in Airflow 3.0, the YAML source will be shown instead of generated Python code"
