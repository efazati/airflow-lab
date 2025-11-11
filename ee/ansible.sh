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

check_active() {
    load_env
    local status=$(ee describe ws "$WS" --json 2>&1 | jq -r '.status // .state // "unknown"')
    [[ "$status" =~ ^(running|active)$ ]] || { echo "❌ Workspace not active"; exit 1; }
}

# ─────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────

cmd_inventory() {
    check_active
    mkdir -p ../inventory

    echo "📝 Generating Ansible inventory..."

    # Generate basic inventory
    {
        echo "[all]"
        grep "^HOSTNAME_" "$ENV_FILE" 2>/dev/null | while IFS= read -r line; do
            name=$(echo "$line" | cut -d= -f1 | sed 's/HOSTNAME_//' | tr '[:upper:]' '[:lower:]' | tr '_' '-')
            host=$(echo "$line" | cut -d= -f2)
            [ -n "${host:-}" ] && echo "$name ansible_host=$host"
        done || true

        echo
        echo "[all:vars]"
        echo "ansible_user=easyenv"
        echo "ansible_ssh_common_args='-o StrictHostKeyChecking=no'"
        echo "ansible_python_interpreter=/usr/bin/python3"
    } > ../inventory/hosts.ini

    # Auto-detect PostgreSQL cluster setup
    local node_count=$(grep "^HOSTNAME_PG_NODE" "$ENV_FILE" 2>/dev/null | wc -l)
    if [ "$node_count" -ge 3 ]; then
        echo
        echo "📦 Detected PostgreSQL cluster setup, adding groups..."
        {
            echo
            echo "# PostgreSQL Cluster Groups"
            echo "[postgres_primary]"
            echo "pg-node-1"
            echo
            echo "[postgres_standby]"
            echo "pg-node-2"
            echo "pg-node-3"
            echo
            echo "[postgres_cluster:children]"
            echo "postgres_primary"
            echo "postgres_standby"
        } >> ../inventory/hosts.ini
        echo "   ✓ Added PostgreSQL cluster groups"
    fi

    echo "✅ Inventory: inventory/hosts.ini"
    echo "   Edit to customize groups if needed"
    echo
    echo "Next: ee/ansible.sh test"
}

cmd_test() {
    check_active
    [ -f "../inventory/hosts.ini" ] || { echo "❌ Run 'ee/ansible.sh inventory' first"; exit 1; }

    command -v ansible &>/dev/null || { echo "❌ Install Ansible first"; exit 1; }

    echo "📡 Testing connectivity..."
    cd ..
    if ansible all -i inventory/hosts.ini -m ping -o; then
        echo
        echo "✅ All nodes reachable"
        echo
        echo "Deploy cluster:"
        echo "  ansible-playbook -i inventory/hosts.ini playbooks/postgres-cluster.yml"
    else
        echo "❌ Connection failed"
        exit 1
    fi
}

cmd_info() {
    check_active
    echo "📊 Ansible Environment"
    echo
    echo "Hosts:"
    grep "^HOSTNAME_" "$ENV_FILE" 2>/dev/null | while IFS= read -r line; do
        name=$(echo "$line" | cut -d= -f1 | sed 's/HOSTNAME_//' | tr '[:upper:]' '[:lower:]' | tr '_' '-')
        host=$(echo "$line" | cut -d= -f2)
        echo "  $name: $host"
    done || true
    echo
    [ -f "../inventory/hosts.ini" ] && echo "✅ Inventory configured" || echo "❌ Inventory not generated"
}

cmd_help() {
    echo "Usage: ee/ansible.sh COMMAND"
    echo
    echo "Commands:"
    echo "  inventory   Generate Ansible inventory file"
    echo "  test        Test connectivity to all nodes"
    echo "  info        Show ansible environment info"
    echo
    echo "Workflow:"
    echo "  ee/setup.sh init && ee/setup.sh vpn"
    echo "  ee/ansible.sh inventory && ee/ansible.sh test"
}

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

case "${1:-}" in
    inventory) cmd_inventory ;;
    test)      cmd_test ;;
    info)      cmd_info ;;
    *)         cmd_help; exit 1 ;;
esac
