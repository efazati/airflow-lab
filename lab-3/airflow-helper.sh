#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Source environment if available
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

log_success() {
    echo -e "${GREEN}✅${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}⚠️${NC} $1"
}

log_error() {
    echo -e "${RED}❌${NC} $1"
}

# Check if kubectl is configured
check_kubectl() {
    if ! kubectl cluster-info &>/dev/null; then
        log_error "kubectl is not configured or cluster is not accessible"
        log_info "Run: source ./ee_setup.sh load_env"
        exit 1
    fi
}

# Command: init
cmd_init() {
    log_info "Initializing Airflow prerequisites..."

    # Add Helm repos
    log_info "Adding Helm repositories..."
    helm repo add airflow-stable https://airflow-helm.github.io/charts
    helm repo update
    log_success "Helm repositories added"

    # Create namespace
    log_info "Creating airflow namespace..."
    kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -
    log_success "Namespace created"

    # Setup logs directory on K3s node
    if [ -n "$WS" ] && [ -n "$BOX_K3S" ]; then
        log_info "Setting up logs directory on K3s node..."
        ee box exec "$WS" "$BOX_K3S" "sudo mkdir -p /opt/airflow-logs && sudo chown -R 50000:0 /opt/airflow-logs && sudo chmod -R 775 /opt/airflow-logs" 2>/dev/null || true
        log_success "Logs directory configured"
    else
        log_warning "WS or BOX_K3S not set, skipping logs directory setup"
        log_info "Run: source ./ee_setup.sh load_env"
    fi

    # Deploy storage
    log_info "Deploying storage (logs PV/PVC)..."
    kubectl apply -f k8s/logs-storage.yaml
    log_success "Storage deployed"

    # Deploy PostgreSQL
    log_info "Deploying PostgreSQL..."
    kubectl apply -f k8s/postgres.yaml
    sleep 5
    log_info "Waiting for PostgreSQL to be ready..."
    kubectl wait --for=condition=ready pod -l app=postgres-external -n airflow --timeout=120s
    log_success "PostgreSQL is ready"

    log_success "Initialization complete!"
    echo ""
    log_info "Next step: $0 install"
}

# Command: install
cmd_install() {
    check_kubectl

    log_info "Installing Airflow 2.10.4 with dag-factory v1.0.1..."

    # Check if already installed
    if helm list -n airflow | grep -q airflow; then
        log_warning "Airflow is already installed"
        read -p "Do you want to upgrade instead? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            helm upgrade airflow airflow-stable/airflow --version 8.9.0 -n airflow -f k8s/airflow.yaml --timeout 10m
            log_success "Airflow upgraded"
        else
            log_info "Skipping installation"
            exit 0
        fi
    else
        helm install airflow airflow-stable/airflow --version 8.9.0 -n airflow -f k8s/airflow.yaml --timeout 10m
        log_success "Airflow installed"
    fi

    log_info "Waiting for pods to be ready..."
    sleep 30
    kubectl wait --for=condition=ready pod -l component=scheduler -n airflow --timeout=300s 2>/dev/null || true

    log_success "Airflow installation complete!"
    echo ""
    log_info "Access UI: $0 serve"
    log_info "Sync DAGs: $0 sync"
    echo ""
    kubectl get pods -n airflow
}

# Command: uninstall
cmd_uninstall() {
    check_kubectl

    log_warning "This will uninstall Airflow and optionally clean up all resources"
    read -p "Continue? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Cancelled"
        exit 0
    fi

    log_info "Uninstalling Airflow..."
    helm uninstall airflow -n airflow 2>/dev/null || log_warning "Airflow not found"
    log_success "Airflow uninstalled"

    read -p "Delete PostgreSQL and storage? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Deleting PostgreSQL..."
        kubectl delete -f k8s/postgres.yaml 2>/dev/null || true
        log_success "PostgreSQL deleted"

        log_info "Deleting storage..."
        kubectl delete -f k8s/logs-storage.yaml 2>/dev/null || true
        log_success "Storage deleted"
    fi

    read -p "Delete namespace? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Deleting namespace..."
        kubectl delete namespace airflow 2>/dev/null || true
        log_success "Namespace deleted"
    fi

    log_success "Cleanup complete!"
}

# Command: sync
cmd_sync() {
    check_kubectl

    log_info "Syncing DAGs to Airflow..."

    # Get running scheduler pods
    SCHEDULERS=($(kubectl get pods -n airflow -l component=scheduler --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}' 2>/dev/null))

    # Get running webserver pods
    WEBSERVERS=($(kubectl get pods -n airflow -l component=web --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}' 2>/dev/null))

    if [ ${#SCHEDULERS[@]} -eq 0 ]; then
        log_error "No running scheduler pods found"
        exit 1
    fi

    log_info "Found ${#SCHEDULERS[@]} scheduler pod(s) and ${#WEBSERVERS[@]} webserver pod(s)"

    # Combine all pods
    ALL_PODS=("${SCHEDULERS[@]}" "${WEBSERVERS[@]}")

    # Copy DAGs to all pods (scheduler + webserver)
    for POD in "${ALL_PODS[@]}"; do
        log_info "Copying DAGs to pod: $POD"

        # Copy all Python files in dags directory
        for dag_file in dags/*.py; do
            if [ -f "$dag_file" ]; then
                kubectl cp "$dag_file" "airflow/$POD:/opt/airflow/dags/" 2>/dev/null || log_warning "Failed to copy $dag_file"
            fi
        done

        # Copy all YAML files
        for yaml_file in dags/*.yml dags/*.yaml; do
            if [ -f "$yaml_file" ]; then
                kubectl cp "$yaml_file" "airflow/$POD:/opt/airflow/dags/" 2>/dev/null || log_warning "Failed to copy $yaml_file"
            fi
        done

        log_success "DAGs copied to $POD"
    done

    # Optional: Restart scheduler pods to force immediate DAG refresh
    read -p "Restart scheduler pods to force immediate DAG refresh? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Restarting scheduler pods..."
        kubectl delete pod -n airflow -l component=scheduler
        sleep 10
        kubectl wait --for=condition=ready pod -l component=scheduler -n airflow --timeout=120s 2>/dev/null || true
        log_success "Schedulers restarted"

        # Re-sync to new pods
        log_info "Re-syncing DAGs to new scheduler pods..."
        sleep 10
        cmd_sync_internal
    fi

    log_success "DAG sync complete!"
    log_info "Wait 30-60 seconds for Airflow to scan for new DAGs"
    echo ""
    log_info "List DAGs: kubectl exec -n airflow \$(kubectl get pods -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}') -- airflow dags list"
}

# Internal sync function (no restart prompt)
cmd_sync_internal() {
    SCHEDULERS=($(kubectl get pods -n airflow -l component=scheduler --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}' 2>/dev/null))
    WEBSERVERS=($(kubectl get pods -n airflow -l component=web --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}' 2>/dev/null))
    ALL_PODS=("${SCHEDULERS[@]}" "${WEBSERVERS[@]}")

    for POD in "${ALL_PODS[@]}"; do
        for dag_file in dags/*.py; do
            [ -f "$dag_file" ] && kubectl cp "$dag_file" "airflow/$POD:/opt/airflow/dags/" 2>/dev/null
        done
        for yaml_file in dags/*.yml dags/*.yaml; do
            [ -f "$yaml_file" ] && kubectl cp "$yaml_file" "airflow/$POD:/opt/airflow/dags/" 2>/dev/null
        done
    done
}

# Command: serve
cmd_serve() {
    check_kubectl

    log_info "Starting port-forward to Airflow webserver..."
    log_info "Access Airflow UI at: http://localhost:8080"
    log_info "Username: admin"
    log_info "Password: admin"
    echo ""
    log_warning "Press Ctrl+C to stop"
    echo ""

    kubectl port-forward -n airflow svc/airflow-web 8080:8080
}

# Command: status
cmd_status() {
    check_kubectl

    echo "==================================="
    echo "  Airflow Deployment Status"
    echo "==================================="
    echo ""

    log_info "Namespace: airflow"
    echo ""

    log_info "Helm Release:"
    helm list -n airflow 2>/dev/null || log_warning "No Helm release found"
    echo ""

    log_info "Pods:"
    kubectl get pods -n airflow 2>/dev/null || log_warning "No pods found"
    echo ""

    log_info "Services:"
    kubectl get svc -n airflow 2>/dev/null || log_warning "No services found"
    echo ""

    log_info "PVCs:"
    kubectl get pvc -n airflow 2>/dev/null || log_warning "No PVCs found"
    echo ""

    # Check if schedulers are running
    RUNNING_SCHEDULERS=$(kubectl get pods -n airflow -l component=scheduler --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | wc -w)
    if [ "$RUNNING_SCHEDULERS" -gt 0 ]; then
        log_success "$RUNNING_SCHEDULERS scheduler pod(s) running"

        POD=$(kubectl get pods -n airflow -l component=scheduler --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
        log_info "DAGs in $POD:"
        kubectl exec -n airflow $POD -- airflow dags list 2>/dev/null | grep -v "DeprecationWarning\|RemovedInAirflow" || log_warning "Failed to list DAGs"
    else
        log_warning "No running scheduler pods found"
    fi
}

# Command: logs
cmd_logs() {
    check_kubectl

    COMPONENT=${1:-scheduler}

    log_info "Fetching logs for component: $COMPONENT"

    POD=$(kubectl get pods -n airflow -l component=$COMPONENT --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

    if [ -z "$POD" ]; then
        log_error "No running $COMPONENT pod found"
        exit 1
    fi

    log_info "Tailing logs from: $POD"
    echo ""
    kubectl logs -n airflow $POD -f --tail=100
}

# Command: list-dags
cmd_list_dags() {
    check_kubectl

    log_info "Listing all DAGs..."

    POD=$(kubectl get pods -n airflow -l component=scheduler --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

    if [ -z "$POD" ]; then
        log_error "No running scheduler pod found"
        exit 1
    fi

    echo ""
    kubectl exec -n airflow $POD -- airflow dags list 2>&1 | grep -v "DeprecationWarning\|RemovedInAirflow"

    echo ""
    log_info "Check for import errors:"
    kubectl exec -n airflow $POD -- airflow dags list-import-errors 2>&1 | grep -v "DeprecationWarning\|RemovedInAirflow"

    echo ""
    log_info "Unpause a DAG: kubectl exec -n airflow $POD -- airflow dags unpause <dag_id>"
    log_info "Trigger a DAG: kubectl exec -n airflow $POD -- airflow dags trigger <dag_id>"
}

# Main command dispatcher
case "${1:-}" in
    init)
        cmd_init
        ;;
    install)
        cmd_install
        ;;
    uninstall)
        cmd_uninstall
        ;;
    sync)
        cmd_sync
        ;;
    serve)
        cmd_serve
        ;;
    status)
        cmd_status
        ;;
    list-dags)
        cmd_list_dags
        ;;
    logs)
        cmd_logs "${2:-scheduler}"
        ;;
    *)
        echo "Airflow Helper Script"
        echo "====================="
        echo ""
        echo "Usage: $0 {init|install|uninstall|sync|serve|status|list-dags|logs}"
        echo ""
        echo "Commands:"
        echo "  init       Initialize prerequisites (repos, postgres, storage)"
        echo "  install    Install Airflow with Helm"
        echo "  uninstall  Uninstall Airflow and optionally clean up resources"
        echo "  sync       Copy DAGs to scheduler pods"
        echo "  serve      Port-forward to access Airflow UI"
        echo "  status     Show deployment status"
        echo "  list-dags  List all DAGs and check for import errors"
        echo "  logs       Tail logs from a component (default: scheduler)"
        echo ""
        echo "Workflow:"
        echo "  1. source ./ee_setup.sh load_env"
        echo "  2. $0 init"
        echo "  3. $0 install"
        echo "  4. $0 sync"
        echo "  5. $0 list-dags"
        echo "  6. $0 serve"
        echo ""
        echo "Examples:"
        echo "  $0 init                  # Setup prerequisites"
        echo "  $0 install               # Install Airflow"
        echo "  $0 sync                  # Sync DAGs"
        echo "  $0 list-dags             # List all DAGs"
        echo "  $0 serve                 # Access UI at localhost:8080"
        echo "  $0 status                # Check deployment status"
        echo "  $0 logs scheduler        # View scheduler logs"
        echo "  $0 logs web              # View webserver logs"
        exit 1
        ;;
esac

