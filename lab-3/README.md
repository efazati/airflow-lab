# Lab 3: Airflow on K3s

Kubernetes-based Airflow 2.10.4 deployment using **Community Helm Chart** with KubernetesExecutor and persistent storage.

## Chart Information

- **Chart**: [airflow-helm/charts](https://github.com/airflow-helm/charts) (Community Chart v8.9.0)
- **Airflow Version**: 2.10.4
- **dag-factory**: v1.0.1 (supports Airflow 2.10+)
- **Why Community Chart?**: Better support for `extraPipPackages` and additional features

## Quick Setup (Using Helper Script) ⚡

```bash
# 1. Initialize workspace
ee/setup.sh init && ee/setup.sh vpn
ee/kube.sh setup
source ee/setup.sh load_env

# 2. Initialize Airflow prerequisites (repos, postgres, storage)
./airflow-helper.sh init

# 3. Install Airflow
./airflow-helper.sh install

# 4. Sync DAGs to Airflow
./airflow-helper.sh sync

# 5. Access the UI
./airflow-helper.sh serve
# Open http://localhost:8080 (admin/admin)
```

## Manual Setup

```bash
# 1. Initialize workspace
cd ee
./ee_setup.sh init && ./ee_setup.sh vpn
./ee_kube.sh setup
source ./ee_setup.sh load_env
cd ..

# 2. Setup logs directory on K3s node (required for persistent logs)
BOX_K3S=$(grep "^BOX_K3S=" ee/.env | cut -d= -f2)
ee box exec "$WS" "$BOX_K3S" "sudo mkdir -p /opt/airflow-logs && sudo chown -R 50000:0 /opt/airflow-logs && sudo chmod -R 775 /opt/airflow-logs"

# 3. Deploy namespace and storage
kubectl create namespace airflow
kubectl apply -f k8s/logs-storage.yaml  # Creates PV and PVC for logs
kubectl apply -f k8s/postgres.yaml

# 4. Install Airflow 2.10.4 with community chart
helm repo add airflow-stable https://airflow-helm.github.io/charts
helm repo update
helm install airflow airflow-stable/airflow --version 8.9.0 -n airflow -f k8s/airflow.yaml --timeout 10m

# 5. Verify & Access
kubectl get pods -n airflow -w
kubectl get pvc -n airflow  # Should show airflow-logs as Bound
kubectl port-forward -n airflow svc/airflow-web 8080:8080
# http://localhost:8080 (admin/admin)
```

## Deploy DAGs

### Method 1: Traditional Python DAGs (✅ Working)

```bash
# Get scheduler pod
POD=$(kubectl get pods -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')

# Copy DAG file
kubectl cp dags/echo_time.py airflow/$POD:/opt/airflow/dags/

# Wait 30-60s for DAG to be parsed

# List DAGs
kubectl exec -n airflow $POD -- airflow dags list

# The DAG will be paused by default, unpause it:
kubectl exec -n airflow $POD -- airflow dags unpause echo_time

# Trigger a run
kubectl exec -n airflow $POD -- airflow dags trigger echo_time
```

### Method 2: Declarative DAGs with dag-factory

```dockerfile
FROM apache/airflow:2.10.4-python3.11

USER root
RUN pip install dag-factory==0.20.2 apache-airflow-providers-cncf-kubernetes
USER airflow
```

Then update `k8s/airflow.yaml`:
```yaml
airflow:
  # Install additional Python packages (including dag-factory)
  extraPipPackages:
    - "dag-factory==1.0.1"
    - "apache-airflow-providers-cncf-kubernetes"
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
kubectl exec -n airflow $POD -- airflow dags list
kubectl exec -n airflow $POD -- airflow dags list-import-errors
kubectl exec -n airflow $POD -- python /opt/airflow/dags/echo_time.py

# Storage
kubectl get pvc -n airflow
kubectl get pv
kubectl get storageclass

# Check logs persistence
kubectl exec -n airflow $POD -- ls -la /opt/airflow/logs/
# On K3s node
BOX_K3S=$(grep "^BOX_K3S=" .env | cut -d= -f2)
ee box exec "$WS" "$BOX_K3S" "sudo ls -la /opt/airflow-logs/"

# Disk space issues
BOX_K3S=$(grep "^BOX_K3S=" .env | cut -d= -f2)
ee box exec "$WS" "$BOX_K3S" "df -h"
# Clean up Docker images if needed:
ee box exec "$WS" "$BOX_K3S" "sudo k3s crictl rmi --prune"
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

## Helper Script Commands

The `airflow-helper.sh` script provides convenient commands for managing your Airflow deployment:

```bash
# Initialize prerequisites (repos, postgres, storage)
./airflow-helper.sh init

# Install Airflow with Helm
./airflow-helper.sh install

# Sync DAGs to all scheduler pods
./airflow-helper.sh sync

# List all DAGs and check for import errors
./airflow-helper.sh list-dags

# Access Airflow UI (port-forward)
./airflow-helper.sh serve

# Check deployment status
./airflow-helper.sh status

# View logs (scheduler, web, triggerer, etc.)
./airflow-helper.sh logs scheduler
./airflow-helper.sh logs web

# Uninstall Airflow (with optional cleanup)
./airflow-helper.sh uninstall
```

### Helper Script Features

- ✅ **Color-coded output** for better readability
- ✅ **Automatic error checking** and validation
- ✅ **Multi-pod sync** - copies DAGs to all scheduler pods
- ✅ **Interactive prompts** for destructive operations
- ✅ **Status monitoring** - check pods, services, DAGs
- ✅ **Log tailing** - view real-time logs from any component
- ✅ **DAG listing** - list all DAGs and check for import errors

## Notes

- DAGs persistence is disabled because local-path storage class doesn't support ReadWriteMany access mode
- DAGs are copied manually to pods (ephemeral storage)
- For persistent DAG storage, use gitSync or NFS with RWX support
- The community chart provides better flexibility and features for advanced use cases
- **dag-factory v1.0.1** works perfectly with Airflow 2.10.4 (use `schedule` instead of `schedule_interval`)
