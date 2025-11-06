#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
ENV_FILE="$SCRIPT_DIR/.env"

# Detect if sourced
sourced=0
[ -n "$ZSH_VERSION" ] && [[ $ZSH_EVAL_CONTEXT =~ :file$ ]] && sourced=1
[ -n "$BASH_VERSION" ] && (return 0 2>/dev/null) && sourced=1

load_env() {
    [ -f "$ENV_FILE" ] || { echo "❌ .env not found. Run './ee_setup.sh init' first."; return 1 2>/dev/null || exit 1; }
    set -a; source "$ENV_FILE"; set +a
    echo "✓ Loaded: WS=$WS"
}

display_env() {
    echo "✅ Environment: WS=$WS"
    echo "Boxes:"; env | grep "^BOX_" | sort | sed 's/^/  /'
    [ -n "$KUBECONFIG" ] && echo "KUBECONFIG: $KUBECONFIG"
}

check_workspace_active() {
    load_env
    echo "🔍 Checking workspace status..."
    WS_STATUS=$(ee describe ws "$WS" --json 2>&1)

    if ! echo "$WS_STATUS" | jq -e . >/dev/null 2>&1; then
        echo "❌ Failed to get workspace status"
        return 1
    fi

    STATUS=$(echo "$WS_STATUS" | jq -r '.status // .state // "unknown"')
    echo "   Status: $STATUS"

    if [[ "$STATUS" != "running" && "$STATUS" != "active" ]]; then
        echo "❌ Workspace is not active (status: $STATUS)"
        echo "   Run './ee_setup.sh init' to create a new workspace or start the existing one with: ee start ws $WS"
        return 1
    fi

    echo "✅ Workspace is active"
    return 0
}

cmd_init() {
    set -e
    echo "🚀 Setting up workspace..."

    echo "📦 Creating workspace..."
    WS=$(ee create ws --config=./ee_conf.yml --json | jq -r '.uuid')
    [ -z "$WS" ] && { echo "❌ Failed to get workspace ID"; exit 1; }
    echo "✅ Workspace: $WS"

    echo "🔄 Starting workspace..."
    ee start ws "$WS"

    echo "⏳ Waiting for workspace..."
    sleep 10

    echo "📦 Extracting box IDs..."
    WS_DESCRIBE=$(ee describe ws "$WS" --json 2>&1)

    echo "WS=$WS" > .env

    if echo "$WS_DESCRIBE" | jq -e . >/dev/null 2>&1; then
        while IFS= read -r box; do
            title=$(echo "$box" | jq -r '.title' | tr '[:lower:]' '[:upper:]' | tr -d ' ' | tr '-' '_')
            id=$(echo "$box" | jq -r '.uuid // .id')
            echo "BOX_$title=$id" >> .env
            echo "  ✅ BOX_$title=$id"
        done < <(echo "$WS_DESCRIBE" | jq -c '.boxes[]')

        # Check for box with export_kubeconfig flag
        KUBE_BOX=$(yq eval '.boxes[] | select(.export_kubeconfig == true) | .title' ./ee_conf.yml | head -1)
        if [ -n "$KUBE_BOX" ]; then
            KUBE_BOX_VAR=$(echo "$KUBE_BOX" | tr '[:lower:]' '[:upper:]' | tr -d ' ' | tr '-' '_')
            KUBE_BOX_ID=$(grep "^BOX_$KUBE_BOX_VAR=" .env | cut -d= -f2)
            echo "BOX_KUBECONFIG=$KUBE_BOX_ID" >> .env
            echo "  ✅ BOX_KUBECONFIG=$KUBE_BOX_ID (from $KUBE_BOX)"
        fi
    else
        echo "❌ Failed to parse workspace"; exit 1
    fi

    echo -e "\n🎉 Complete! Next: ./ee_setup.sh vpn"
}

cmd_vpn() {
    check_workspace_active || exit 1
    echo "🌐 Connecting VPN for WS=$WS..."

    if ! command -v netbird &> /dev/null; then
        echo "❌ NetBird not installed. Run: curl -fsSL https://pkgs.netbird.io/install.sh | sh"
        exit 1
    fi

    # Clean NetBird config for fresh connection
    echo "🧹 Cleaning NetBird config..."
    netbird down 2>/dev/null || true
    sudo rm -f /var/lib/netbird/config.json

    # Connect to workspace
    ee connect ws "$WS"

    # Wait for DNS to propagate
    echo "⏳ Waiting for DNS to propagate..."
    sleep 5

    # Verify DNS is working by getting first box hostname
    BOX_FIRST=$(grep "^BOX_" "$ENV_FILE" | head -1 | cut -d= -f2)
    if [ -n "$BOX_FIRST" ]; then
        BOX_HOSTNAME=$(ee box hostname "$WS" "$BOX_FIRST")
        echo "🔍 Testing DNS for: $BOX_HOSTNAME"

        # Try to resolve DNS (retry up to 10 times)
        for i in {1..10}; do
            if ping -c 1 -W 2 "$BOX_HOSTNAME" &>/dev/null || \
               host "$BOX_HOSTNAME" &>/dev/null || \
               nslookup "$BOX_HOSTNAME" &>/dev/null 2>&1; then
                echo "✅ DNS resolved successfully!"
                break
            fi
            echo "   Attempt $i/10: DNS not ready yet, waiting..."
            sleep 2
        done
    fi

    echo "✅ VPN connected and ready. Next: ./ee_setup.sh kube"
}

cmd_kube() {
    check_workspace_active || exit 1
    BOX_KUBECONFIG=$(grep "^BOX_KUBECONFIG=" "$ENV_FILE" | cut -d= -f2)
    [ -z "$BOX_KUBECONFIG" ] && { echo "❌ No box configured for kubeconfig export. Add 'export_kubeconfig: true' to a box in ee_conf.yml"; exit 1; }

    echo "🔧 Exporting kubeconfig (WS=$WS, Box=$BOX_KUBECONFIG)..."
    echo "🔑 Copying SSH key..."
    ee box ssh-copy-id "$WS" "$BOX_KUBECONFIG"

    BOX_HOSTNAME=$(ee box hostname "$WS" "$BOX_KUBECONFIG")
    ee box exec "$WS" "$BOX_KUBECONFIG" "kubectl config view --raw" | \
        sed "s/127.0.0.1/$BOX_HOSTNAME/g" | \
        sed '/certificate-authority-data:/d' | \
        sed '/server:/a\    insecure-skip-tls-verify: true' > kubeconfig.yaml

    if ! grep -q "^KUBECONFIG=" "$ENV_FILE"; then
        echo -e "\nKUBECONFIG=$SCRIPT_DIR/kubeconfig.yaml\nBOX_KUBECONFIG_HOSTNAME=$BOX_HOSTNAME" >> .env
    fi

    export KUBECONFIG="$SCRIPT_DIR/kubeconfig.yaml"
    echo "✅ Kubeconfig exported: $SCRIPT_DIR/kubeconfig.yaml"
    echo "Testing cluster..."; kubectl get nodes
    echo -e "\n🎉 Ready! Run: source ./ee_setup.sh load_env"
}

cmd_cleanup() {
    load_env
    echo "🧹 Cleaning workspace: $WS"
    ee stop ws "$WS" || true

    read -p "Delete workspace? (y/N): " -n 1 -r; echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        ee delete ws "$WS"
        rm -f "$SCRIPT_DIR/.env" "$SCRIPT_DIR/kubeconfig.yaml"
        echo "✅ Deleted"
    else
        echo "✅ Stopped only"
    fi
}

cmd_load_env() {
    if [ "$sourced" -eq 1 ]; then
        load_env; display_env
        echo "✅ Exported to shell!"
    else
        load_env; display_env
        echo "⚠️  Run: source ./ee_setup.sh load_env"
        return 1 2>/dev/null || exit 1
    fi
}

cmd_workspace() {
    load_env
    echo "📋 Workspace Details:"
    ee describe ws "$WS"
}

cmd_status() {
    if [ -f "$ENV_FILE" ]; then
        load_env; display_env
        [ -f "kubeconfig.yaml" ] && echo "✅ Kubeconfig exists" || echo "❌ Kubeconfig missing"
    else
        echo "❌ Not initialized. Run './ee_setup.sh init'"
    fi
}

case "${1:-}" in
    init) cmd_init ;;
    vpn) cmd_vpn ;;
    kube) cmd_kube ;;
    cleanup) cmd_cleanup ;;
    load_env) cmd_load_env ;;
    workspace) cmd_workspace ;;
    status) cmd_status ;;
    *)
        echo "Usage: $0 {init|vpn|kube|cleanup|load_env|workspace|status}"
        echo ""
        echo "Commands:"
        echo "  init       Create and start workspace"
        echo "  vpn        Connect to workspace VPN"
        echo "  kube       Export kubeconfig"
        echo "  cleanup    Stop/delete workspace"
        echo "  load_env   Load environment (use: source ./ee_setup.sh load_env)"
        echo "  workspace  Show workspace details"
        echo "  status     Show status"
        echo ""
        echo "Workflow:"
        echo "  ./ee_setup.sh init"
        echo "  ./ee_setup.sh vpn"
        echo "  ./ee_setup.sh kube"
        echo "  source ./ee_setup.sh load_env"
        exit 1
        ;;
esac
