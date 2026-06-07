#!/usr/bin/env bash
#
# AFD - Auto Download Tool
# One-click install script
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/LL264a/afd/main/install.sh | bash
#   or
#   curl -fsSL https://raw.githubusercontent.com/LL264a/afd/main/install.sh | bash -s -- --bindir /usr/local/bin
#

set -euo pipefail

REPO="LL264a/afd"
BINDIR="${BINDIR:-/usr/local/bin}"
BINARY_NAME="afd"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bindir) BINDIR="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--bindir DIR]"
            echo "  --bindir DIR  Installation directory (default: /usr/local/bin)"
            exit 0
            ;;
        *) error "Unknown option: $1" ;;
    esac
done

# Detect OS and Architecture
detect_platform() {
    OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
    ARCH="$(uname -m)"

    case "$OS" in
        linux)  OS="linux" ;;
        darwin) OS="darwin" ;;
        freebsd) OS="freebsd" ;;
        *) error "Unsupported OS: $OS" ;;
    esac

    case "$ARCH" in
        x86_64|amd64)  ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        armv7l|armv7)  ARCH="arm" ;;
        i386|i686)     ARCH="386" ;;
        *) error "Unsupported architecture: $ARCH" ;;
    esac

    if [[ "$OS" == "windows"* || "$OS" == "MINGW"* || "$OS" == "MSYS"* ]]; then
        error "This script is for Unix-like systems. On Windows, download from https://github.com/${REPO}/releases"
    fi
}

# Get the latest release tag
get_latest_version() {
    info "Checking latest version..."
    VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null | grep '"tag_name"' | head -1 | sed -E 's/.*"([^"]+)".*/\1/')
    if [[ -z "$VERSION" ]]; then
        # Fallback: list releases and pick the first one
        VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases" 2>/dev/null | grep '"tag_name"' | head -1 | sed -E 's/.*"([^"]+)".*/\1/')
    fi
    if [[ -z "$VERSION" ]]; then
        error "Could not determine latest version. Check your network or GitHub API rate limit."
    fi
    info "Latest version: ${VERSION}"
}

# Download and install
install_afd() {
    local EXT=""
    local FILENAME="${BINARY_NAME}-${OS}-${ARCH}${EXT}"

    local DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${FILENAME}"
    local CHECKSUM_URL="https://github.com/${REPO}/releases/download/${VERSION}/${FILENAME}.sha256"

    local TMPDIR
    TMPDIR=$(mktemp -d)
    trap 'rm -rf "$TMPDIR"' EXIT

    # Download binary
    info "Downloading ${FILENAME}..."
    curl -fSL --progress-bar -o "${TMPDIR}/${FILENAME}" "${DOWNLOAD_URL}" || {
        error "Download failed! The platform ${OS}-${ARCH} may not be available."
    }

    # Download checksum and verify
    info "Verifying checksum..."
    if curl -fSL -o "${TMPDIR}/${FILENAME}.sha256" "${CHECKSUM_URL}" 2>/dev/null; then
        EXPECTED=$(cut -d' ' -f1 < "${TMPDIR}/${FILENAME}.sha256")
        ACTUAL=$(sha256sum "${TMPDIR}/${FILENAME}" | cut -d' ' -f1)
        if [[ "$EXPECTED" != "$ACTUAL" ]]; then
            error "Checksum mismatch! Expected: ${EXPECTED}, Got: ${ACTUAL}"
        fi
        success "Checksum verified"
    else
        warn "Checksum file not available, skipping verification"
    fi

    # Make executable
    chmod +x "${TMPDIR}/${FILENAME}"

    # Install
    info "Installing to ${BINDIR}/${BINARY_NAME}..."
    if [[ -w "$BINDIR" ]]; then
        mv "${TMPDIR}/${FILENAME}" "${BINDIR}/${BINARY_NAME}"
    else
        sudo mv "${TMPDIR}/${FILENAME}" "${BINDIR}/${BINARY_NAME}"
    fi

    success "Installed ${BINARY_NAME} to ${BINDIR}/${BINARY_NAME}"
}

# Verify installation
verify_installation() {
    if command -v "${BINARY_NAME}" &>/dev/null; then
        success "AFD installed successfully!"
        echo ""
        "${BINARY_NAME}" --version 2>/dev/null || echo "  Version: ${VERSION}"
        echo ""
        echo "Quick start:"
        echo "  afd dl <URL>                    # Download a file"
        echo "  afd dl --adaptive <URL>         # Adaptive thread download"
        echo "  afd dl -s 4 -o file.zip <URL>   # 4 threads, specify output"
        echo "  afd dl -i urls.txt -d ./downloads  # Batch download"
    else
        warn "${BINDIR} is not in your PATH. Add it with:"
        echo "  export PATH=\"${BINDIR}:\$PATH\""
    fi
}

# Main
main() {
    echo ""
    echo "  _   _      _ _         ___        _   "
    echo " | \\ | |    | | |       |__ \\      | |  "
    echo " |  \\| | ___| | |_ ___     ) |___  | |_ "
    echo " | . \` |/ _ \\ | __/ _ \\   / /___ \\ | __|"
    echo " | |\\  |  __/ | || (_) | |____| || | | "
    echo "  \\_| \\_/\\___|_|\\__\\___/       |_||_| "
    echo ""
    echo "  Installer"
    echo ""

    detect_platform
    info "Platform: ${OS}-${ARCH}"
    get_latest_version
    install_afd
    verify_installation
}

main
