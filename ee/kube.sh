#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-${(%):-%x}}")" && pwd)"
cd "$SCRIPT_DIR"
ENV_FILE="$SCRIPT_DIR/.env"

set -eo pipefail

load_env() {
    [ -f "$ENV_FILE" ] || { echo "❌ .env not found. Run 'ee/setup.sh init' first"; exit 1; }
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
}

check_kubectl() {
    [ -n "${KUBECONFIG:-}" ] || { echo "❌ KUBECONFIG not set. Run: source ee/setup.sh load_env"; exit 1; }
    kubectl cluster-info &>/dev/null || { echo "❌ Cannot connect to cluster"; exit 1; }
}

# ─────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────

cmd_setup() {
    load_env
    local box=$(grep "^BOX_KUBECONFIG=" "$ENV_FILE" 2>/dev/null | cut -d= -f2)
    [ -z "$box" ] && { echo "❌ No kubeconfig box in workspace"; exit 1; }

    echo "🔧 Exporting kubeconfig..."
    ee box ssh-copy-id "$WS" "$box" &>/dev/null
    local hostname=$(ee box hostname "$WS" "$box")

    ee box exec "$WS" "$box" "kubectl config view --raw" | \
        sed "s/127.0.0.1/$hostname/g" | \
        sed '/certificate-authority-data:/d' | \
        sed '/server:/a\    insecure-skip-tls-verify: true' > kubeconfig.yaml

    grep -q "^KUBECONFIG=" "$ENV_FILE" 2>/dev/null || echo "KUBECONFIG=$SCRIPT_DIR/kubeconfig.yaml" >> "$ENV_FILE"

    export KUBECONFIG="$SCRIPT_DIR/kubeconfig.yaml"
    echo "✅ Kubeconfig: $SCRIPT_DIR/kubeconfig.yaml"
    echo
    kubectl get nodes 2>/dev/null || true
}

cmd_info() {
    load_env
    check_kubectl

    echo "📊 Kubernetes Environment"
    echo
    echo "Cluster:"
    kubectl cluster-info | head -2
    echo
    echo "Nodes:"
    kubectl get nodes -o wide
    echo
    echo "Kubeconfig: $KUBECONFIG"
}

cmd_ns() {
    load_env
    check_kubectl

    if [ -n "${1:-}" ]; then
        echo "🔄 Switching to namespace: $1"
        kubectl config set-context --current --namespace="$1"
    else
        echo "📋 Namespaces:"
        kubectl get namespaces
        echo
        echo "Current: $(kubectl config view --minify --output 'jsonpath={..namespace}')"
        echo
        echo "Switch: ee/kube.sh ns <namespace>"
    fi
}

cmd_pods() {
    load_env
    check_kubectl

    local ns="${1:-}"
    if [ -n "$ns" ]; then
        kubectl get pods -n "$ns" -o wide
    else
        kubectl get pods --all-namespaces -o wide
    fi
}

cmd_help() {
    echo "Usage: ee/kube.sh COMMAND [args]"
    echo
    echo "Setup:"
    echo "  setup           Export kubeconfig from workspace"
    echo
    echo "Info Commands:"
    echo "  info            Show cluster and node info"
    echo "  ns [name]       List namespaces or switch to one"
    echo "  pods [ns]       List pods (all namespaces or specific)"
    echo
    echo "Examples:"
    echo "  ee/kube.sh setup"
    echo "  ee/kube.sh info"
    echo "  ee/kube.sh ns airflow"
    echo "  ee/kube.sh pods airflow"
}

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

case "${1:-}" in
    setup)   cmd_setup ;;
    info)    cmd_info ;;
    ns)      cmd_ns "${2:-}" ;;
    pods)    cmd_pods "${2:-}" ;;
    *)       cmd_help; exit 1 ;;
esac
