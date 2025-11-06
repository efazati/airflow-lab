# Lab 3: Kubernetes Workspace

Kubernetes cluster workspace with modular setup automation.

## Quick Start

```bash
./ee_setup.sh init           # Create and start workspace
./ee_setup.sh vpn            # Connect to VPN
./ee_setup.sh kube           # Export kubeconfig
source ./ee_setup.sh load_env # Load environment to shell
kubectl get nodes
./ee_setup.sh cleanup
```

## Airflow

```bash
# Load environment
source ./ee_setup.sh load_env

# Deploy PostgreSQL
kubectl create namespace airflow
kubectl apply -f k8s/postgres.yaml

# Install Airflow
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --values k8s/airflow.yaml \
  --timeout 10m

# Access UI
  kubectl port-forward -n airflow svc/airflow-api-server 8080:8080
  # http://localhost:8080 (admin/admin)
```

## Deploy DAGs

```bash
# Get scheduler and dag-processor pods
SCHEDULER_POD=$(kubectl get pods -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
DAG_PROCESSOR_POD=$(kubectl get pods -n airflow -l component=dag-processor -o jsonpath='{.items[0].metadata.name}')

# Copy DAG to both pods
kubectl cp dags/echo_time.py airflow/$SCHEDULER_POD:/opt/airflow/dags/ -c scheduler
kubectl cp dags/echo_time.py airflow/$DAG_PROCESSOR_POD:/opt/airflow/dags/ -c dag-processor

# Verify DAGs are copied
kubectl exec -n airflow $SCHEDULER_POD -c scheduler -- ls -la /opt/airflow/dags/
kubectl exec -n airflow $DAG_PROCESSOR_POD -c dag-processor -- ls -la /opt/airflow/dags/

# Check for DAG parsing errors
kubectl logs -n airflow $DAG_PROCESSOR_POD -c dag-processor --tail=100 | grep -i "error\|exception\|echo_time"

# Wait 30-60 seconds for Airflow to parse the DAG, then check UI
```

### Troubleshooting

```bash
# Check what path dag-processor is looking at
kubectl exec -n airflow $DAG_PROCESSOR_POD -c dag-processor -- env | grep DAG

# Check actual dags folder contents
kubectl exec -n airflow $DAG_PROCESSOR_POD -c dag-processor -- ls -la /opt/airflow/dags/

# Check if file has correct permissions
kubectl exec -n airflow $DAG_PROCESSOR_POD -c dag-processor -- cat /opt/airflow/dags/echo_time.py

# Check DAG processor config
kubectl exec -n airflow $DAG_PROCESSOR_POD -c dag-processor -- airflow config get-value core dags_folder

# List all DAGs (should show echo_time)
kubectl exec -n airflow $DAG_PROCESSOR_POD -c dag-processor -- airflow dags list

# Check processor logs for parsing errors
kubectl logs -n airflow $DAG_PROCESSOR_POD -c dag-processor --tail=200

# Test DAG syntax
kubectl exec -n airflow $DAG_PROCESSOR_POD -c dag-processor -- python /opt/airflow/dags/echo_time.py
```

### Example DAG: echo_time.py
- Runs every minute
- Echoes current timestamp
- Located in `dags/echo_time.py`
