# Lab 3: Airflow on K3s

Kubernetes-based Airflow 2.10.5 deployment with KubernetesExecutor and persistent storage.

## Quick Setup

```bash
# 1. Initialize workspace
./ee_setup.sh init && ./ee_setup.sh vpn && ./ee_setup.sh kube
source ./ee_setup.sh load_env

# 2. Setup logs directory on K3s node (required for persistent logs)
BOX_K3S=$(grep "^BOX_K3S=" .env | cut -d= -f2)
ee box exec "$WS" "$BOX_K3S" "sudo mkdir -p /opt/airflow-logs && sudo chown -R 50000:0 /opt/airflow-logs && sudo chmod -R 775 /opt/airflow-logs"

# 3. Deploy namespace and storage
kubectl create namespace airflow
kubectl apply -f k8s/logs-storage.yaml  # Creates PV and PVC for logs
kubectl apply -f k8s/postgres.yaml

# 4. Install Airflow 2.10.5 with persistent logs
helm repo add apache-airflow https://airflow.apache.org && helm repo update
helm install airflow apache-airflow/airflow --version 1.16.0 -n airflow -f k8s/airflow.yaml --timeout 10m

# 5. Verify & Access
kubectl get pods -n airflow -w
kubectl get pvc -n airflow  # Should show airflow-logs as Bound
kubectl port-forward -n airflow svc/airflow-webserver 8080:8080
# http://localhost:8080 (admin/admin)
```

## Deploy DAGs

```bash
POD=$(kubectl get pods -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
kubectl cp dags/echo_time.py airflow/$POD:/opt/airflow/dags/ -c scheduler
# Wait 30-60s, DAG appears in UI
```
## Logs Persistence

Logs are stored on the K3s node at `/opt/airflow-logs` using hostPath volumes. This works for single-node clusters and persists logs across pod restarts.

**For Multi-Node Production:**
- Use remote logging (S3/GCS/Azure Blob)
- Use NFS with proper network setup
- Logs are accessible from UI even after task pods terminate

## Troubleshooting

```bash
# Pods
kubectl get pods -n airflow
kubectl logs -n airflow <pod> -c <container>
kubectl describe pod -n airflow <pod>

# DAGs
POD=$(kubectl get pods -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n airflow $POD -c scheduler -- airflow dags list
kubectl exec -n airflow $POD -c scheduler -- python /opt/airflow/dags/echo_time.py

# Storage
kubectl get pvc -n airflow
kubectl get pv
kubectl get storageclass

# Check logs persistence
kubectl exec -n airflow $POD -c scheduler -- ls -la /opt/airflow/logs/
# On K3s node
BOX_K3S=$(grep "^BOX_K3S=" .env | cut -d= -f2)
ee box exec "$WS" "$BOX_K3S" "sudo ls -la /opt/airflow-logs/"
```

## Cleanup

```bash
# Uninstall Airflow
helm uninstall airflow -n airflow

# Delete storage and PostgreSQL
kubectl delete -f k8s/logs-storage.yaml
kubectl delete -f k8s/postgres.yaml

# (Optional) Delete logs from K3s node
BOX_K3S=$(grep "^BOX_K3S=" .env | cut -d= -f2)
ee box exec "$WS" "$BOX_K3S" "sudo rm -rf /opt/airflow-logs"

# Delete namespace
kubectl delete namespace airflow

# Cleanup workspace
./ee_setup.sh cleanup
```

## Files

| File | Description |
|------|-------------|
| `k8s/airflow.yaml` | Airflow Helm values (version 2.10.5, logs persistence enabled) |
| `k8s/logs-storage.yaml` | **PV + PVC for logs** (hostPath, RWX) |
| `k8s/postgres.yaml` | PostgreSQL database deployment |
| `dags/echo_time.py` | Example DAG (runs hourly, Airflow 2.x syntax) |
| `ee_setup.sh` | Workspace automation (init, vpn, kube) |

## Architecture

- **Executor**: KubernetesExecutor (each task runs in separate pod)
- **DAGs Storage**: PersistentVolume (local-path, RWO) - shared across scheduler
- **Logs Storage**: PersistentVolume (hostPath, RWX) - persists after pod termination
- **Database**: PostgreSQL (external deployment)
- **Version**: Airflow 2.10.5 (pinned)
