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
BACKUP_DIR="${INSTALL_DIR}/backups"
SYSTEMD_DIR="/etc/systemd/system"
GIT_REPO="https://github.com/Cordtus/yaci-explorer-apis.git"
GIT_BRANCH="${GIT_BRANCH:-main}"

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
    install             Full installation (deps, app, config, migrations, services)
    update              Update app from local files and restart services
    deploy [branch]     Deploy from git (default: main) with backup and migrations
    migrate             Run database migrations only
    backup              Create database backup
    restore <file>      Restore database from backup
    rollback <dir>      Rollback to previous deployment
    config              Setup configuration files
    start               Start all services
    stop                Stop all services
    restart             Restart all services
    status              Show service status
    logs                Follow chain params daemon logs
    logs-evm            Follow EVM decode daemon logs
    enable-evm          Enable EVM decoder services
    help                Show this help

Examples:
    sudo $0 install             # First time setup
    sudo $0 deploy              # Deploy latest main from git
    sudo $0 deploy feat/ibc     # Deploy specific branch
    sudo $0 backup              # Backup database before manual changes
    sudo $0 restore             # List available backups
    sudo $0 rollback            # List available rollback points
    sudo $0 migrate             # Run migrations only
    sudo $0 logs                # View logs

Environment:
    GIT_BRANCH=<branch>  Override default branch for deploy command

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
    cp -r packages migrations proto scripts package.json bun.lock tsconfig.json "${INSTALL_DIR}/"

    cd "${INSTALL_DIR}"
    bun install

    info "Restarting services..."
    systemctl restart yaci-chain-params 2>/dev/null || true
    systemctl restart yaci-evm-decode 2>/dev/null || true
    systemctl restart yaci-evm-priority 2>/dev/null || true

    success "Update complete"
    show_status
}

backup_db() {
    info "Creating database backup..."
    mkdir -p "${BACKUP_DIR}"

    if [ -z "$DATABASE_URL" ]; then
        if [ -f "${CONFIG_DIR}/explorer-apis.env" ]; then
            source "${CONFIG_DIR}/explorer-apis.env"
        fi
    fi

    if [ -z "$DATABASE_URL" ]; then
        error "DATABASE_URL not set"
    fi

    BACKUP_FILE="${BACKUP_DIR}/backup_$(date +%Y%m%d_%H%M%S).dump"
    pg_dump -Fc "$DATABASE_URL" > "$BACKUP_FILE"
    success "Backup created: $BACKUP_FILE"

    # Keep only last 5 backups
    cd "${BACKUP_DIR}"
    ls -t *.dump 2>/dev/null | tail -n +6 | xargs -r rm -f
    info "Retained last 5 backups"
}

restore_db() {
    check_root

    if [ -z "$2" ]; then
        info "Available backups:"
        ls -lt "${BACKUP_DIR}"/*.dump 2>/dev/null || echo "  No backups found"
        echo ""
        error "Usage: $0 restore <backup_file>"
    fi

    BACKUP_FILE="$2"
    if [ ! -f "$BACKUP_FILE" ]; then
        # Try relative to backup dir
        BACKUP_FILE="${BACKUP_DIR}/$2"
    fi

    if [ ! -f "$BACKUP_FILE" ]; then
        error "Backup file not found: $2"
    fi

    if [ -z "$DATABASE_URL" ]; then
        if [ -f "${CONFIG_DIR}/explorer-apis.env" ]; then
            source "${CONFIG_DIR}/explorer-apis.env"
        fi
    fi

    warning "This will REPLACE the current database with backup: $BACKUP_FILE"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        info "Aborted"
        exit 0
    fi

    stop_services
    info "Restoring database..."
    pg_restore -Fc -c -d "$DATABASE_URL" "$BACKUP_FILE"
    start_services
    success "Database restored from $BACKUP_FILE"
}

deploy_from_git() {
    check_root
    check_deps

    BRANCH="${2:-$GIT_BRANCH}"
    DEPLOY_TMP="/tmp/yaci-deploy-$$"

    info "Deploying from git (branch: $BRANCH)..."

    # Backup first
    backup_db

    # Clone to temp
    git clone --depth 1 --branch "$BRANCH" "$GIT_REPO" "$DEPLOY_TMP"

    # Stop services
    stop_services

    # Save current version for rollback
    if [ -d "${INSTALL_DIR}/.git" ] || [ -f "${INSTALL_DIR}/package.json" ]; then
        ROLLBACK_DIR="${BACKUP_DIR}/rollback_$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$ROLLBACK_DIR"
        cp -r "${INSTALL_DIR}/packages" "${INSTALL_DIR}/migrations" "${INSTALL_DIR}/scripts" \
              "${INSTALL_DIR}/package.json" "${INSTALL_DIR}/bun.lock" "$ROLLBACK_DIR/" 2>/dev/null || true
        info "Saved rollback point: $ROLLBACK_DIR"
    fi

    # Update application
    cp -r "$DEPLOY_TMP/packages" "$DEPLOY_TMP/migrations" "$DEPLOY_TMP/proto" \
          "$DEPLOY_TMP/scripts" "$DEPLOY_TMP/package.json" "$DEPLOY_TMP/bun.lock" \
          "$DEPLOY_TMP/tsconfig.json" "${INSTALL_DIR}/"

    cd "${INSTALL_DIR}"
    bun install

    # Run migrations
    run_migrations

    # Restart services
    start_services

    # Cleanup
    rm -rf "$DEPLOY_TMP"

    success "Deployment complete (branch: $BRANCH)"
    show_status
}

rollback() {
    check_root

    ROLLBACK_DIRS=$(ls -dt "${BACKUP_DIR}"/rollback_* 2>/dev/null | head -5)

    if [ -z "$ROLLBACK_DIRS" ]; then
        error "No rollback points found"
    fi

    if [ -z "$2" ]; then
        info "Available rollback points:"
        ls -dt "${BACKUP_DIR}"/rollback_* 2>/dev/null | head -5
        echo ""
        error "Usage: $0 rollback <rollback_dir>"
    fi

    ROLLBACK_DIR="$2"
    if [ ! -d "$ROLLBACK_DIR" ]; then
        ROLLBACK_DIR="${BACKUP_DIR}/$2"
    fi

    if [ ! -d "$ROLLBACK_DIR" ]; then
        error "Rollback directory not found: $2"
    fi

    warning "Rolling back to: $ROLLBACK_DIR"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        info "Aborted"
        exit 0
    fi

    stop_services

    cp -r "$ROLLBACK_DIR"/* "${INSTALL_DIR}/"
    cd "${INSTALL_DIR}"
    bun install

    start_services
    success "Rolled back to $ROLLBACK_DIR"
    show_status
}

case "${1:-help}" in
    install)
        full_install
        ;;
    update)
        update_app
        ;;
    deploy)
        deploy_from_git "$@"
        ;;
    migrate)
        check_root
        run_migrations
        ;;
    backup)
        check_root
        backup_db
        ;;
    restore)
        restore_db "$@"
        ;;
    rollback)
        rollback "$@"
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
