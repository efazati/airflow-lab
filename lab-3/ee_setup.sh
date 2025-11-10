#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
ENV_FILE="$SCRIPT_DIR/.env"

# Detect if sourced
sourced=0
[ -n "${ZSH_VERSION:-}" ] && [[ ${ZSH_EVAL_CONTEXT:-} =~ :file$ ]] && sourced=1
[ -n "${BASH_VERSION:-}" ] && (return 0 2>/dev/null) && sourced=1

set -euo pipefail

load_env() {
    [ -f "$ENV_FILE" ] || { echo "❌ .env not found. Run './ee_setup.sh init'"; return 1 2>/dev/null || exit 1; }
    set -a; source "$ENV_FILE"; set +a
}

check_active() {
    load_env
    local status=$(ee describe ws "$WS" --json 2>&1 | jq -r '.status // .state // "unknown"')
    [[ "$status" =~ ^(running|active)$ ]] || { echo "❌ Workspace not active"; exit 1; }
}

detect_type() {
    [ -f "./ee_conf.yml" ] && grep -q "export_kubeconfig.*true" ./ee_conf.yml 2>/dev/null && echo "k8s" && return
    echo "ansible"
}

# ─────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────

cmd_init() {
    echo "🚀 Creating workspace..."
    local ws=$(ee create ws --config=./ee_conf.yml --json | jq -r '.uuid')
    [ -z "$ws" ] && { echo "❌ Failed to create workspace"; exit 1; }

    echo "WS=$ws" > .env
    ee start ws "$ws"
    sleep 10

    echo "📦 Extracting box info..."
    ee describe ws "$ws" --json | jq -rc '.boxes[]' | while read -r box; do
        local title=$(echo "$box" | jq -r '.title' | tr '[:lower:]' '[:upper:]' | tr -d ' ' | tr '-' '_')
        local id=$(echo "$box" | jq -r '.uuid // .id')
        echo "BOX_$title=$id" >> .env
    done

    # Check for kubeconfig box
    if command -v yq &>/dev/null; then
        local kube_box=$(yq -r '.boxes[] | select(.export_kubeconfig == true) | .title' ./ee_conf.yml 2>/dev/null | head -1)
        if [ -n "$kube_box" ]; then
            local kube_var=$(echo "$kube_box" | tr '[:lower:]' '[:upper:]' | tr -d ' ' | tr '-' '_')
            local kube_id=$(grep "^BOX_$kube_var=" .env | cut -d= -f2)
            echo "BOX_KUBECONFIG=$kube_id" >> .env
        fi
    fi

    echo "✅ Workspace: $ws"
    echo "Next: ./ee_setup.sh vpn"
}

cmd_vpn() {
    check_active
    command -v netbird &>/dev/null || { echo "❌ Install NetBird: curl -fsSL https://pkgs.netbird.io/install.sh | sh"; exit 1; }

    echo "🌐 Connecting VPN..."
    netbird down 2>/dev/null || true
    sudo rm -f /var/lib/netbird/config.json
    ee connect ws "$WS"
    sleep 5

    # Test DNS
    local box_id=$(grep "^BOX_" "$ENV_FILE" | head -1 | cut -d= -f2)
    local hostname=$(ee box hostname "$WS" "$box_id")
    for i in {1..10}; do
        ping -c1 -W2 "$hostname" &>/dev/null && break
        [ $i -eq 10 ] && echo "⚠️  DNS might not be ready"
        sleep 2
    done

    echo "✅ VPN connected"
    [ "$(detect_type)" = "k8s" ] && echo "Next: ./ee_setup.sh kube" || echo "Next: ./ee_setup.sh ssh"
}

cmd_kube() {
    check_active
    local box=$(grep "^BOX_KUBECONFIG=" "$ENV_FILE" 2>/dev/null | cut -d= -f2)
    [ -z "$box" ] && { echo "❌ No kubeconfig box configured"; exit 1; }

    echo "🔧 Exporting kubeconfig..."
    ee box ssh-copy-id "$WS" "$box" &>/dev/null
    local hostname=$(ee box hostname "$WS" "$box")

    ee box exec "$WS" "$box" "kubectl config view --raw" | \
        sed "s/127.0.0.1/$hostname/g" | \
        sed '/certificate-authority-data:/d' | \
        sed '/server:/a\    insecure-skip-tls-verify: true' > kubeconfig.yaml

    grep -q "^KUBECONFIG=" "$ENV_FILE" || echo -e "\nKUBECONFIG=$SCRIPT_DIR/kubeconfig.yaml" >> .env

    export KUBECONFIG="$SCRIPT_DIR/kubeconfig.yaml"
    echo "✅ Kubeconfig: $SCRIPT_DIR/kubeconfig.yaml"
    kubectl get nodes
}

cmd_ssh() {
    check_active
    echo "🔑 Setting up SSH..."

    # Copy SSH keys
    grep "^BOX_" "$ENV_FILE" | grep -v "BOX_KUBECONFIG" | while read -r line; do
        local id=$(echo "$line" | cut -d= -f2)
        ee box ssh-copy-id "$WS" "$id" &>/dev/null
    done

    # Save hostnames with .meshnet.local
    sed -i '/^HOSTNAME_/d' "$ENV_FILE"
    grep "^BOX_" "$ENV_FILE" | grep -v "BOX_KUBECONFIG" | while read -r line; do
        local name=$(echo "$line" | cut -d= -f1 | sed 's/BOX_//')
        local id=$(echo "$line" | cut -d= -f2)
        local hostname=$(ee box hostname "$WS" "$id")
        [[ ! "$hostname" =~ \.meshnet\.local$ ]] && hostname="${hostname}.meshnet.local"
        echo "HOSTNAME_$name=$hostname" >> "$ENV_FILE"
    done

    echo "✅ SSH configured"
    echo "Next: ./ee_setup.sh inventory"
}

cmd_inventory() {
    check_active
    mkdir -p inventory

    # Build simple inventory from workspace boxes
    {
        echo "[all]"

        # Add all boxes to inventory
        grep "^HOSTNAME_" "$ENV_FILE" | while read -r line; do
            local name=$(echo "$line" | cut -d= -f1 | sed 's/HOSTNAME_//' | tr '[:upper:]' '[:lower:]' | tr '_' '-')
            local host=$(echo "$line" | cut -d= -f2)
            echo "$name ansible_host=$host"
        done

        echo
        echo "[all:vars]"
        echo "ansible_user=easyenv"
        echo "ansible_ssh_common_args='-o StrictHostKeyChecking=no'"
        echo "ansible_python_interpreter=/usr/bin/python3"
    } > inventory/hosts.ini

    echo "✅ Inventory: inventory/hosts.ini (edit to add custom groups)"
    echo "Next: ./ee_setup.sh ansible"
}

cmd_ansible() {
    check_active
    command -v ansible &>/dev/null || pip install ansible
    [ -f "inventory/hosts.ini" ] || { echo "❌ Run './ee_setup.sh inventory' first"; exit 1; }

    echo "📡 Testing connectivity..."
    if ansible all -i inventory/hosts.ini -m ping -o; then
        echo "✅ All nodes reachable"
        [ -f "playbooks/postgres-cluster.yml" ] && echo "Deploy: ansible-playbook -i inventory/hosts.ini playbooks/postgres-cluster.yml"
    else
        echo "❌ Connection failed"
        exit 1
    fi
}

cmd_cleanup() {
    load_env
    ee stop ws "$WS" || true
    read -p "Delete workspace? (y/N): " -n 1 -r; echo
    [[ $REPLY =~ ^[Yy]$ ]] && {
        ee delete ws "$WS"
        rm -f .env kubeconfig.yaml inventory/hosts.ini
        echo "✅ Deleted"
    } || echo "✅ Stopped"
}

cmd_load_env() {
    if [ "$sourced" -eq 1 ]; then
        load_env
        echo "✅ WS=$WS"
        env | grep -E "^(BOX_|HOSTNAME_|KUBECONFIG)" | sort
    else
        echo "⚠️  Run: source ./ee_setup.sh load_env"
        exit 1
    fi
}

cmd_status() {
    if [ -f "$ENV_FILE" ]; then
        load_env
        echo "WS: $WS"
        [ -f "kubeconfig.yaml" ] && echo "✅ Kubeconfig" || echo "❌ No kubeconfig"
        [ -f "inventory/hosts.ini" ] && echo "✅ Inventory" || echo "❌ No inventory"
    else
        echo "❌ Not initialized"
    fi
}

cmd_help() {
    local type=$(detect_type)
    echo "Usage: ./ee_setup.sh COMMAND"
    echo
    echo "Common:"
    echo "  init        Create workspace"
    echo "  vpn         Connect VPN"
    echo "  status      Show status"
    echo "  workspace   Show detailed workspace info"
    echo "  cleanup     Stop/delete workspace"
    echo "  load_env    Load env vars (use: source ./ee_setup.sh load_env)"
    echo

    if [ "$type" = "k8s" ]; then
        echo "Kubernetes:"
        echo "  kube        Export kubeconfig"
        echo
        echo "Workflow:"
        echo "  ./ee_setup.sh init && ./ee_setup.sh vpn && ./ee_setup.sh kube"
    else
        echo "Ansible:"
        echo "  ssh         Setup SSH keys"
        echo "  inventory   Generate inventory"
        echo "  ansible     Test connectivity"
        echo
        echo "Kubernetes:"
        echo "  kube        Export kubeconfig (if configured)"
        echo
        echo "Workflow:"
        echo "  ./ee_setup.sh init && ./ee_setup.sh vpn && ./ee_setup.sh ssh && ./ee_setup.sh inventory"
    fi
}

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

case "${1:-}" in
    init)      cmd_init ;;
    vpn)       cmd_vpn ;;
    kube)      cmd_kube ;;
    ssh)       cmd_ssh ;;
    inventory) cmd_inventory ;;
    ansible)   cmd_ansible ;;
    cleanup)   cmd_cleanup ;;
    load_env)  cmd_load_env ;;
    workspace) load_env && ee describe ws "$WS" ;;
    status)    cmd_status ;;
    *)         cmd_help; exit 1 ;;
esac
