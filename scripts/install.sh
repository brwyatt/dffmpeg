#!/bin/bash
set -e

# DFFmpeg Installer Script
# This script automates the installation of DFFmpeg components (Coordinator, Worker, Client).
# It handles dependencies, fetches releases from GitHub, and sets up virtual environments.

# Configuration
REPO="brwyatt/dffmpeg"
INSTALL_DIR="/opt/dffmpeg"
GITHUB_API_URL="https://api.github.com/repos/$REPO/releases"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1" >&2; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1" >&2; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "Please run as root."
        exit 1
    fi
}

install_deps() {
    log_info "Checking dependencies..."
    
    if command -v apt-get &> /dev/null; then
        # Debian/Ubuntu
        log_info "Detected apt-based system."
        apt-get update -qq
        apt-get install -y -qq python3-venv curl python3-pip
    elif command -v dnf &> /dev/null; then
        # Fedora/RHEL/CentOS
        log_info "Detected dnf-based system."
        dnf install -y python3 curl
    elif command -v pacman &> /dev/null; then
        # Arch Linux
        log_info "Detected pacman-based system."
        pacman -Sy --noconfirm python curl
    elif command -v zypper &> /dev/null; then
        # OpenSUSE
        log_info "Detected zypper-based system."
        zypper install -y python3 curl
    else
        log_warn "Could not detect package manager. Ensure python3-venv and curl are installed manually."
    fi

    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is required but not found."
        exit 1
    fi
}

get_releases() {
    log_info "Fetching available releases..."
    RESPONSE=$(curl -s "$GITHUB_API_URL")
    
    # Use python to parse JSON and output "tag_name"
    echo "$RESPONSE" | python3 -c "import sys, json; print('\n'.join([r['tag_name'] for r in json.load(sys.stdin)]))"
}

select_version() {
    local releases=($(get_releases))
    
    if [ ${#releases[@]} -eq 0 ]; then
        log_error "No releases found."
        exit 1
    fi

    echo "Available versions:"
    for i in "${!releases[@]}"; do
        echo "$((i+1)). ${releases[$i]}"
    done

    read -p "Select version [1]: " choice
    choice=${choice:-1}
    
    # Validate input
    if ! [[ "$choice" =~ ^[0-9]+$ ]] || [ "$choice" -lt 1 ] || [ "$choice" -gt "${#releases[@]}" ]; then
        log_error "Invalid selection."
        exit 1
    fi

    SELECTED_VERSION="${releases[$((choice-1))]}"
    log_info "Selected version: $SELECTED_VERSION"
}

install_component() {
    local component=$1
    local version_tag=$2
    local install_path="$INSTALL_DIR/$component"
    
    # Clean version string for filename (extract X.Y.Z from tag)
    local version_clean=$(echo "$version_tag" | sed -E 's/^v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
    
    # Package names use underscores
    local package_name="dffmpeg_$component"
    local common_package="dffmpeg_common"
    
    # Construct download URLs
    # Assuming standard wheel naming: package-version-py3-none-any.whl
    local base_url="https://github.com/$REPO/releases/download/$version_tag"
    local component_whl="$package_name-$version_clean-py3-none-any.whl"
    local common_whl="$common_package-$version_clean-py3-none-any.whl"
    
    local component_url="$base_url/$component_whl"
    local common_url="$base_url/$common_whl"

    log_info "Installing $component..."
    
    # Create venv
    if [ -d "$install_path" ]; then
        log_warn "Directory $install_path already exists. Updating..."
    else
        log_info "Creating virtual environment at $install_path..."
        python3 -m venv "$install_path"
    fi
    
    # Download and Install
    # Create a temp dir for downloads
    local tmp_dir=$(mktemp -d)
    
    log_info "Downloading artifacts..."
    
    # Download common first
    if ! curl -L -f -o "$tmp_dir/$common_whl" "$common_url"; then
        log_error "Failed to download $common_whl from $common_url"
        rm -rf "$tmp_dir"
        return 1
    fi

    # Download component
    if ! curl -L -f -o "$tmp_dir/$component_whl" "$component_url"; then
        log_error "Failed to download $component_whl from $component_url"
        rm -rf "$tmp_dir"
        return 1
    fi
    
    log_info "Installing packages into venv..."
    "$install_path/bin/pip" install "$tmp_dir/$common_whl" "$tmp_dir/$component_whl"
    
    # Cleanup
    rm -rf "$tmp_dir"
    
    log_info "Successfully installed $component to $install_path"
}

main() {
    check_root
    install_deps
    
    select_version
    
    echo "Select components to install:"
    echo "1. Coordinator"
    echo "2. Worker"
    echo "3. Client"
    echo "4. All"
    read -p "Selection [4]: " comp_choice
    comp_choice=${comp_choice:-4}

    mkdir -p "$INSTALL_DIR"

    case $comp_choice in
        1)
            install_component "coordinator" "$SELECTED_VERSION"
            ;;
        2)
            install_component "worker" "$SELECTED_VERSION"
            ;;
        3)
            install_component "client" "$SELECTED_VERSION"
            ;;
        4)
            install_component "coordinator" "$SELECTED_VERSION"
            install_component "worker" "$SELECTED_VERSION"
            install_component "client" "$SELECTED_VERSION"
            ;;
        *)
            log_error "Invalid selection."
            exit 1
            ;;
    esac

    echo ""
    log_info "Installation complete!"
    echo "Make sure to create configuration files for your installed components."
    echo "Example: $INSTALL_DIR/coordinator/bin/dffmpeg-coordinator"
    echo ""
}

main
