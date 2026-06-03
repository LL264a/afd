#!/bin/bash

set -e

VERSION="1.0.0"
BINARY_NAME="afd"
INSTALL_DIR="/usr/local/bin"
DATA_DIR="$HOME/.afd"
CONFIG_FILE="$DATA_DIR/config.yaml"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_go() {
    if command -v go &> /dev/null; then
        GO_VERSION=$(go version | grep -oP 'go\K[0-9]+\.[0-9]+')
        log_info "Go installed: $GO_VERSION"
        return 0
    else
        log_warn "Go not installed, installing..."
        return 1
    fi
}

install_go() {
    log_info "Installing Go..."

    if [[ "$(uname -m)" == "x86_64" ]]; then
        ARCH="amd64"
    elif [[ "$(uname -m)" == "aarch64" ]]; then
        ARCH="arm64"
    else
        log_error "Unsupported architecture: $(uname -m)"
        exit 1
    fi

    GO_VERSION="1.25.0"
    wget -q "https://go.dev/dl/go${GO_VERSION}.linux-${ARCH}.tar.gz" -O /tmp/go.tar.gz

    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf /tmp/go.tar.gz
    rm /tmp/go.tar.gz

    export PATH=$PATH:/usr/local/go/bin
    echo "export PATH=\$PATH:/usr/local/go/bin" >> ~/.bashrc

    log_info "Go installed"
}

install_afd() {
    log_info "Building AFD..."

    cd /tmp
    rm -rf afd
    git clone https://github.com/nexus-dl/afd.git || {
        log_error "Failed to download source"
        exit 1
    }

    cd afd

    go build -o "$BINARY_NAME" ./cmd/afd

    sudo mv "$BINARY_NAME" "$INSTALL_DIR/"
    sudo chmod +x "$INSTALL_DIR/$BINARY_NAME"

    cd ~
    rm -rf afd

    log_info "AFD installed: $INSTALL_DIR/$BINARY_NAME"
}

create_config() {
    mkdir -p "$DATA_DIR"

    if [[ ! -f "$CONFIG_FILE" ]]; then
        cat > "$CONFIG_FILE" << EOF
node:
  id: "node-$(date +%s)"
  name: "AFD Node"
  data_dir: "$DATA_DIR/downloads"
  log_level: "info"

api:
  host: "0.0.0.0"
  port: 8080
  cors_enabled: true

cluster:
  enabled: true
  grpc_port: 9999
  discovery_enabled: true

download:
  max_connections: 16
  buffer_size: 1048576
  timeout: 300
  retry_count: 3
  max_connections_per_server: 8
EOF
        log_info "Config created: $CONFIG_FILE"
    fi
}

start_afd() {
    log_info "Starting AFD..."

    if pgrep -x "$BINARY_NAME" > /dev/null; then
        log_warn "AFD already running"
        return
    fi

    nohup "$BINARY_NAME" serve --config "$CONFIG_FILE" > "$DATA_DIR/afd.log" 2>&1 &

    sleep 2

    if pgrep -x "$BINARY_NAME" > /dev/null; then
        log_info "AFD started!"
        log_info "API: http://localhost:8080"
        log_info "Log: $DATA_DIR/afd.log"
    else
        log_error "Failed to start, check log: $DATA_DIR/afd.log"
    fi
}

stop_afd() {
    log_info "Stopping AFD..."
    pkill -x "$BINARY_NAME" 2>/dev/null || true
    log_info "AFD stopped"
}

show_status() {
    if pgrep -x "$BINARY_NAME" > /dev/null; then
        log_info "AFD: Running"
        log_info "API: http://localhost:8080"
    else
        log_info "AFD: Not running"
    fi
}

show_help() {
    echo "AFD - Auto Download Tool"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  install     Install AFD (auto install Go)"
    echo "  start       Start AFD"
    echo "  stop        Stop AFD"
    echo "  restart     Restart AFD"
    echo "  status      Show status"
    echo "  uninstall   Uninstall AFD"
    echo ""
}

case "${1:-install}" in
    install)
        if ! check_go; then
            install_go
        fi
        install_afd
        create_config
        start_afd
        ;;
    start)
        start_afd
        ;;
    stop)
        stop_afd
        ;;
    restart)
        stop_afd
        sleep 1
        start_afd
        ;;
    status)
        show_status
        ;;
    uninstall)
        stop_afd
        sudo rm -f "$INSTALL_DIR/$BINARY_NAME"
        rm -rf "$DATA_DIR"
        log_info "AFD uninstalled"
        ;;
    *)
        show_help
        ;;
esac