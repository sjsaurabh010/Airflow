#!/bin/bash
################################################################################
# SSH Key Setup Helper Script
# Simplifies SSH key generation and distribution to remote workers
################################################################################

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

usage() {
    cat << EOF
Usage: $0 <worker_host> <worker_user> [options]

Setup SSH keys for passwordless authentication to remote workers

Arguments:
    worker_host     Remote worker IP or hostname
    worker_user     SSH username on remote worker

Options:
    -p, --port      SSH port (default: 22)
    -k, --key       SSH key path (default: ~/.ssh/airflow_worker_key)
    -h, --help      Show this help message

Examples:
    # Setup SSH key for worker at 45.151.155.74
    $0 45.151.155.74 airflow

    # With custom port
    $0 10.0.1.20 airflow --port 2222

    # With custom key path
    $0 10.0.1.20 airflow --key ~/.ssh/custom_key

EOF
    exit 1
}

################################################################################
# Parse arguments
################################################################################
WORKER_HOST=""
WORKER_USER=""
SSH_PORT=22
SSH_KEY_PATH="$HOME/.ssh/airflow_worker_key"

while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--port)
            SSH_PORT="$2"
            shift 2
            ;;
        -k|--key)
            SSH_KEY_PATH="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            if [ -z "$WORKER_HOST" ]; then
                WORKER_HOST="$1"
            elif [ -z "$WORKER_USER" ]; then
                WORKER_USER="$1"
            else
                log_error "Unknown argument: $1"
                usage
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [ -z "$WORKER_HOST" ] || [ -z "$WORKER_USER" ]; then
    log_error "Missing required arguments"
    usage
fi

################################################################################
# Main execution
################################################################################
log_info "=========================================="
log_info "SSH Key Setup for Remote Worker"
log_info "=========================================="
log_info "Worker Host: ${WORKER_HOST}"
log_info "Worker User: ${WORKER_USER}"
log_info "SSH Port:    ${SSH_PORT}"
log_info "Key Path:    ${SSH_KEY_PATH}"
log_info "=========================================="
echo ""

################################################################################
# Step 1: Generate SSH key if not exists
################################################################################
log_step "Checking SSH key..."

if [ -f "${SSH_KEY_PATH}" ]; then
    log_info "✅ SSH key already exists: ${SSH_KEY_PATH}"
    log_warn "Skipping key generation (key already exists)"
else
    log_info "Generating new SSH key..."
    
    # Create .ssh directory if not exists
    mkdir -p "$(dirname ${SSH_KEY_PATH})"
    chmod 700 "$(dirname ${SSH_KEY_PATH})"
    
    # Generate key
    ssh-keygen -t rsa -b 4096 -f "${SSH_KEY_PATH}" -N "" -C "airflow-worker-key"
    
    if [ $? -eq 0 ]; then
        log_info "✅ SSH key generated successfully"
        
        # Set correct permissions
        chmod 600 "${SSH_KEY_PATH}"
        chmod 644 "${SSH_KEY_PATH}.pub"
        
        log_info "Key location: ${SSH_KEY_PATH}"
        log_info "Public key:   ${SSH_KEY_PATH}.pub"
    else
        log_error "Failed to generate SSH key"
        exit 1
    fi
fi

echo ""

################################################################################
# Step 2: Test SSH connection
################################################################################
log_step "Testing SSH connection to ${WORKER_HOST}..."

# Try connection (will prompt for password if not already setup)
if ssh -o BatchMode=yes -o ConnectTimeout=5 -p ${SSH_PORT} -i "${SSH_KEY_PATH}" "${WORKER_USER}@${WORKER_HOST}" "echo 'OK'" 2>/dev/null | grep -q "OK"; then
    log_info "✅ SSH key authentication already working!"
    log_info "No further action needed."
    exit 0
else
    log_warn "SSH key authentication not setup yet"
    log_info "Will copy public key to remote worker..."
fi

echo ""

################################################################################
# Step 3: Copy public key to remote worker
################################################################################
log_step "Copying public key to remote worker..."
log_warn "You will be prompted for the password of ${WORKER_USER}@${WORKER_HOST}"
echo ""

# Use ssh-copy-id if available
if command -v ssh-copy-id &> /dev/null; then
    ssh-copy-id -i "${SSH_KEY_PATH}.pub" -p ${SSH_PORT} "${WORKER_USER}@${WORKER_HOST}"
    
    if [ $? -eq 0 ]; then
        log_info "✅ Public key copied successfully"
    else
        log_error "Failed to copy public key"
        exit 1
    fi
else
    # Manual method
    log_warn "ssh-copy-id not available, using manual method..."
    
    # Read public key
    PUB_KEY=$(cat "${SSH_KEY_PATH}.pub")
    
    # Copy to remote
    ssh -p ${SSH_PORT} "${WORKER_USER}@${WORKER_HOST}" "
        mkdir -p ~/.ssh
        chmod 700 ~/.ssh
        echo '${PUB_KEY}' >> ~/.ssh/authorized_keys
        chmod 600 ~/.ssh/authorized_keys
        echo 'Public key added'
    "
    
    if [ $? -eq 0 ]; then
        log_info "✅ Public key copied successfully"
    else
        log_error "Failed to copy public key"
        exit 1
    fi
fi

echo ""

################################################################################
# Step 4: Verify passwordless SSH
################################################################################
log_step "Verifying passwordless SSH connection..."

if ssh -o BatchMode=yes -p ${SSH_PORT} -i "${SSH_KEY_PATH}" "${WORKER_USER}@${WORKER_HOST}" "echo 'Connection successful'" 2>/dev/null | grep -q "Connection successful"; then
    log_info "✅ Passwordless SSH authentication verified!"
else
    log_error "Passwordless SSH verification failed"
    log_error "Please check:"
    log_error "  1. SSH key permissions (private: 600, public: 644)"
    log_error "  2. Remote ~/.ssh/authorized_keys permissions (600)"
    log_error "  3. Remote ~/.ssh directory permissions (700)"
    log_error "  4. SSH daemon configuration (PubkeyAuthentication yes)"
    exit 1
fi

echo ""

################################################################################
# Step 5: Test tunneling capability
################################################################################
log_step "Testing SSH port forwarding capability..."

# Test local port forwarding
TEST_PORT=19999
ssh -f -N -L ${TEST_PORT}:localhost:22 -p ${SSH_PORT} -i "${SSH_KEY_PATH}" "${WORKER_USER}@${WORKER_HOST}" 2>/dev/null

if [ $? -eq 0 ]; then
    log_info "✅ SSH port forwarding works!"
    
    # Kill test tunnel
    pkill -f "ssh.*-L.*${TEST_PORT}" 2>/dev/null
    
    log_info "Worker is ready for Airflow remote execution"
else
    log_warn "⚠️ SSH port forwarding test failed"
    log_warn "This might be disabled on the remote server"
    log_warn "Check /etc/ssh/sshd_config for 'AllowTcpForwarding yes'"
fi

echo ""

################################################################################
# Summary
################################################################################
log_info "=========================================="
log_info "✅ SSH Setup Completed Successfully!"
log_info "=========================================="
log_info ""
log_info "Worker connection details:"
log_info "  Host: ${WORKER_HOST}"
log_info "  User: ${WORKER_USER}"
log_info "  Port: ${SSH_PORT}"
log_info "  Key:  ${SSH_KEY_PATH}"
log_info ""
log_info "Next steps:"
log_info "  1. Deploy worker launcher script:"
log_info "     scp -i ${SSH_KEY_PATH} scripts/worker_launcher.sh ${WORKER_USER}@${WORKER_HOST}:/home/airflow/"
log_info ""
log_info "  2. Add worker to registry:"
log_info "     python scripts/manage_workers.py add worker-1 ${WORKER_HOST} --user ${WORKER_USER}"
log_info ""
log_info "  3. Test with example DAG:"
log_info "     airflow dags trigger example_dag_with_workers"
log_info ""
log_info "=========================================="
