#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-${(%):-%x}}")" && pwd)"
cd "$SCRIPT_DIR"
ENV_FILE="$SCRIPT_DIR/.env"

# Detect if sourced
sourced=0
[ -n "${ZSH_VERSION:-}" ] && [[ ${ZSH_EVAL_CONTEXT:-} =~ :file$ ]] && sourced=1
[ -n "${BASH_VERSION:-}" ] && (return 0 2>/dev/null) && sourced=1

# Use errexit and pipefail, but NOT nounset (too strict for env files)
set -eo pipefail

load_env() {
    [ -f "$ENV_FILE" ] || { echo "❌ .env not found. Run './setup.sh init'"; return 1 2>/dev/null || exit 1; }
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

cmd_init() {
    [ -f "./conf.yml" ] || { echo "❌ conf.yml not found"; exit 1; }

    echo "🚀 Creating workspace..."
    ws=$(ee create ws --config=./conf.yml --json | jq -r '.uuid')
    [ -z "$ws" ] && { echo "❌ Failed to create workspace"; exit 1; }

    echo "WS=$ws" > "$ENV_FILE"
    ee start ws "$ws"
    sleep 10

    echo "📦 Extracting box info..."
    ee describe ws "$ws" --json | jq -rc '.boxes[]' | while IFS= read -r box; do
        title=$(echo "$box" | jq -r '.title' | tr '[:lower:]' '[:upper:]' | tr -d ' ' | tr '-' '_')
        id=$(echo "$box" | jq -r '.uuid // .id')
        echo "BOX_$title=$id" >> "$ENV_FILE"
    done

    # Detect kubeconfig box
    if command -v yq &>/dev/null && [ -f "./conf.yml" ]; then
        kube_box=$(yq -r '.boxes[] | select(.export_kubeconfig == true) | .title' ./conf.yml 2>/dev/null | head -1)
        if [ -n "${kube_box:-}" ]; then
            kube_var=$(echo "$kube_box" | tr '[:lower:]' '[:upper:]' | tr -d ' ' | tr '-' '_')
            kube_id=$(grep "^BOX_$kube_var=" "$ENV_FILE" | cut -d= -f2)
            echo "BOX_KUBECONFIG=$kube_id" >> "$ENV_FILE"
        fi
    fi

    echo "✅ Workspace: $ws"
    echo "Next: ./setup.sh vpn"
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

    # Setup SSH keys and save hostnames
    echo "🔑 Setting up SSH access..."
    sed -i '/^HOSTNAME_/d' "$ENV_FILE" 2>/dev/null || true
    grep "^BOX_" "$ENV_FILE" | while IFS= read -r line; do
        name=$(echo "$line" | cut -d= -f1 | sed 's/BOX_//')
        id=$(echo "$line" | cut -d= -f2)
        [ -z "$id" ] && continue
        echo "Setting up SSH for: $name"
        ee box ssh-copy-id "$WS" "$id"
        host=$(ee box hostname "$WS" "$id")
        [[ ! "$host" =~ \.meshnet\.local$ ]] && host="${host}.meshnet.local"
        echo "HOSTNAME_$name=$host" >> "$ENV_FILE"
    done

    echo "✅ VPN connected and SSH configured"
}

cmd_cleanup() {
    load_env
    echo "🛑 Stopping workspace: $WS"
    ee stop ws "$WS" || true

    echo
    read -p "Delete workspace files (.env, kubeconfig)? (y/N): " -n 1 -r; echo
    [[ $REPLY =~ ^[Yy]$ ]] && {
        rm -f "$ENV_FILE" kubeconfig.yaml
        echo "✅ Workspace stopped and files deleted"
        echo "ℹ️  To delete the workspace permanently, use the EasyEnv web UI"
    } || {
        echo "✅ Workspace stopped (files kept)"
    }
}

cmd_load_env() {
    if [ "$sourced" -eq 1 ]; then
        load_env
        echo "✅ Environment loaded"
        env | grep -E "^(WS|BOX_|HOSTNAME_|KUBECONFIG)=" 2>/dev/null | sort || true
    else
        echo "⚠️  Run: source ./setup.sh load_env"
        exit 1
    fi
}

cmd_status() {
    if [ -f "$ENV_FILE" ]; then
        load_env
        echo "📊 Workspace: $WS"
        local status=$(ee describe ws "$WS" --json 2>&1 | jq -r '.status // .state // "unknown"')
        echo "   Status: $status"
        grep -q "^HOSTNAME_" "$ENV_FILE" && echo "   ✅ SSH configured"
        echo
        echo "Boxes:"
        grep "^BOX_" "$ENV_FILE" 2>/dev/null | while IFS= read -r line; do
            name=$(echo "$line" | cut -d= -f1 | sed 's/BOX_//' | tr '[:upper:]' '[:lower:]' | tr '_' '-')
            echo "   - $name"
        done || true
    else
        echo "❌ Not initialized (run: ./setup.sh init)"
    fi
}

cmd_help() {
    echo "Usage: ee/setup.sh COMMAND"
    echo
    echo "Commands:"
    echo "  init        Create workspace"
    echo "  vpn         Connect VPN and setup SSH"
    echo "  status      Show status"
    echo "  workspace   Show detailed workspace info"
    echo "  cleanup     Stop/delete workspace"
    echo "  load_env    Load env vars (use: source ./setup.sh load_env)"
    echo
    echo "Workflow:"
    echo "  ee/setup.sh init && ee/setup.sh vpn"
    echo "  source ee/setup.sh load_env"
    echo
    echo "Project-specific helpers:"
    echo "  ee/ansible.sh - Ansible inventory and connectivity"
    echo "  ee/kube.sh    - Kubernetes helpers"
}

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

case "${1:-}" in
    init)      cmd_init ;;
    vpn)       cmd_vpn ;;
    cleanup)   cmd_cleanup ;;
    load_env)  cmd_load_env ;;
    workspace) load_env && ee describe ws "$WS" ;;
    status)    cmd_status ;;
    *)         cmd_help; exit 1 ;;
esac
