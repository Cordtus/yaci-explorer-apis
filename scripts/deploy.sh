#!/bin/bash
# YACI Explorer APIs Deployment Script
# Handles installation, migration, and service management

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
INSTALL_DIR="/opt/yaci-explorer-apis"
CONFIG_DIR="${INSTALL_DIR}/config"
SYSTEMD_DIR="/etc/systemd/system"

info() { echo -e "${BLUE}[i]${NC} $1"; }
success() { echo -e "${GREEN}[+]${NC} $1"; }
warning() { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[x]${NC} $1"; exit 1; }

check_root() {
    if [ "$EUID" -ne 0 ]; then
        error "This script must be run as root. Use: sudo $0"
    fi
}

check_deps() {
    if ! command -v bun &> /dev/null; then
        error "Bun is required. Install with: curl -fsSL https://bun.sh/install | bash"
    fi
}

install_app() {
    info "Installing application to ${INSTALL_DIR}..."
    mkdir -p "${INSTALL_DIR}"
    mkdir -p "${CONFIG_DIR}"

    # Copy application files
    cp -r packages migrations proto scripts package.json bun.lock tsconfig.json "${INSTALL_DIR}/"

    # Install dependencies
    cd "${INSTALL_DIR}"
    bun install
    success "Application installed"
}

setup_config() {
    info "Setting up configuration..."

    if [ ! -f "${CONFIG_DIR}/explorer-apis.env" ]; then
        cat > "${CONFIG_DIR}/explorer-apis.env" << 'EOF'
# YACI Explorer APIs Configuration

# PostgreSQL connection string (same database as yaci indexer)
DATABASE_URL=postgres://yaci:password@localhost:5432/yaci?sslmode=disable

# gRPC endpoint for chain params daemon
CHAIN_GRPC_ENDPOINT=localhost:9090

# Set to 'true' for insecure gRPC (no TLS)
YACI_INSECURE=false

# Chain params polling interval (ms)
CHAIN_PARAMS_POLL_INTERVAL_MS=60000

# EVM decoder polling interval (ms)
POLL_INTERVAL_MS=5000
EOF
        warning "Created ${CONFIG_DIR}/explorer-apis.env - PLEASE EDIT THIS FILE"
    else
        info "Using existing config at ${CONFIG_DIR}/explorer-apis.env"
    fi
    success "Configuration setup complete"
}

run_migrations() {
    info "Running database migrations..."

    if [ -z "$DATABASE_URL" ]; then
        if [ -f "${CONFIG_DIR}/explorer-apis.env" ]; then
            source "${CONFIG_DIR}/explorer-apis.env"
        fi
    fi

    if [ -z "$DATABASE_URL" ]; then
        error "DATABASE_URL not set. Configure ${CONFIG_DIR}/explorer-apis.env first."
    fi

    cd "${INSTALL_DIR}"
    DATABASE_URL="$DATABASE_URL" ./scripts/migrate.sh
    success "Migrations complete"
}

install_services() {
    info "Installing systemd services..."

    # Chain Params Daemon
    cat > "${SYSTEMD_DIR}/yaci-chain-params.service" << EOF
[Unit]
Description=YACI Chain Params Daemon
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${CONFIG_DIR}/explorer-apis.env
ExecStart=/usr/bin/bun run scripts/chain-params-daemon.ts
Restart=on-failure
RestartSec=10s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=yaci-chain-params

[Install]
WantedBy=multi-user.target
EOF

    # EVM Decode Daemon
    cat > "${SYSTEMD_DIR}/yaci-evm-decode.service" << EOF
[Unit]
Description=YACI EVM Decode Daemon
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${CONFIG_DIR}/explorer-apis.env
ExecStart=/usr/bin/bun run scripts/decode-evm-daemon.ts
Restart=on-failure
RestartSec=10s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=yaci-evm-decode

[Install]
WantedBy=multi-user.target
EOF

    # EVM Priority Decoder
    cat > "${SYSTEMD_DIR}/yaci-evm-priority.service" << EOF
[Unit]
Description=YACI EVM Priority Decoder
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${CONFIG_DIR}/explorer-apis.env
ExecStart=/usr/bin/bun run scripts/decode-evm-single.ts
Restart=on-failure
RestartSec=10s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=yaci-evm-priority

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    success "Systemd services installed"
}

enable_services() {
    info "Enabling services..."
    systemctl enable yaci-chain-params
    # Only enable EVM services if needed
    # systemctl enable yaci-evm-decode
    # systemctl enable yaci-evm-priority
    success "Services enabled"
}

start_services() {
    info "Starting services..."
    systemctl restart yaci-chain-params
    sleep 2
    if systemctl is-active --quiet yaci-chain-params; then
        success "Chain params daemon started"
    else
        error "Chain params daemon failed to start. Check: journalctl -u yaci-chain-params -n 50"
    fi
}

stop_services() {
    info "Stopping services..."
    systemctl stop yaci-chain-params 2>/dev/null || true
    systemctl stop yaci-evm-decode 2>/dev/null || true
    systemctl stop yaci-evm-priority 2>/dev/null || true
    success "Services stopped"
}

show_status() {
    echo ""
    info "Service Status:"
    echo ""
    echo "Chain Params Daemon:"
    systemctl status yaci-chain-params --no-pager 2>/dev/null || echo "  Not installed"
    echo ""
    echo "EVM Decode Daemon:"
    systemctl status yaci-evm-decode --no-pager 2>/dev/null || echo "  Not installed"
    echo ""
    echo "EVM Priority Decoder:"
    systemctl status yaci-evm-priority --no-pager 2>/dev/null || echo "  Not installed"
}

show_help() {
    cat << EOF
YACI Explorer APIs Deployment Script

Usage: $0 [COMMAND]

Commands:
    install         Full installation (deps, app, config, migrations, services)
    update          Update app and restart services
    migrate         Run database migrations only
    config          Setup configuration files
    start           Start all services
    stop            Stop all services
    restart         Restart all services
    status          Show service status
    logs            Follow chain params daemon logs
    logs-evm        Follow EVM decode daemon logs
    enable-evm      Enable EVM decoder services
    help            Show this help

Examples:
    sudo $0 install     # First time setup
    sudo $0 update      # After code changes
    sudo $0 migrate     # Run migrations only
    sudo $0 logs        # View logs

Configuration:
    Edit ${CONFIG_DIR}/explorer-apis.env
EOF
}

full_install() {
    check_root
    check_deps

    echo ""
    info "Starting YACI Explorer APIs Installation..."
    echo ""

    install_app
    setup_config
    install_services
    enable_services

    echo ""
    warning "IMPORTANT: Edit ${CONFIG_DIR}/explorer-apis.env before starting!"
    warning "Configure DATABASE_URL and CHAIN_GRPC_ENDPOINT"
    echo ""

    read -p "Have you configured the env file? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        run_migrations
        start_services
        show_status
    else
        warning "Run migrations manually: sudo $0 migrate"
        warning "Then start services: sudo $0 start"
    fi

    echo ""
    success "Installation complete!"
}

update_app() {
    check_root

    info "Updating YACI Explorer APIs..."

    # Copy updated files
    cp -r packages migrations proto scripts package.json yarn.lock tsconfig.json "${INSTALL_DIR}/"

    cd "${INSTALL_DIR}"
    yarn install --production=false

    info "Restarting services..."
    systemctl restart yaci-chain-params 2>/dev/null || true
    systemctl restart yaci-evm-decode 2>/dev/null || true
    systemctl restart yaci-evm-priority 2>/dev/null || true

    success "Update complete"
    show_status
}

case "${1:-help}" in
    install)
        full_install
        ;;
    update)
        update_app
        ;;
    migrate)
        check_root
        run_migrations
        ;;
    config)
        check_root
        setup_config
        ;;
    start)
        check_root
        start_services
        ;;
    stop)
        check_root
        stop_services
        ;;
    restart)
        check_root
        stop_services
        start_services
        ;;
    status)
        show_status
        ;;
    logs)
        journalctl -u yaci-chain-params -f
        ;;
    logs-evm)
        journalctl -u yaci-evm-decode -f
        ;;
    enable-evm)
        check_root
        systemctl enable yaci-evm-decode
        systemctl enable yaci-evm-priority
        systemctl start yaci-evm-decode
        systemctl start yaci-evm-priority
        success "EVM services enabled and started"
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        error "Unknown command: $1. Use '$0 help' for usage."
        ;;
esac
