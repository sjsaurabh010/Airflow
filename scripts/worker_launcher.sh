#!/bin/bash
################################################################################
# Worker Launcher Script
# Deploy this script to remote workers at: /home/airflow/worker_launcher.sh
# 
# This script:
# - Sets up SSH tunnels to master node
# - Starts Celery worker (on-demand)
# - Connects to master via tunnels (PostgreSQL, Redis)
# - Stops when tasks complete
################################################################################

set -e

# Configuration (set via environment or defaults)
WORKER_NAME="${WORKER_NAME:-worker-default}"
QUEUE_NAME="${QUEUE_NAME:-default}"
MASTER_IP="${MASTER_IP:-localhost}"
AIRFLOW_HOME="${AIRFLOW_HOME:-/home/airflow/airflow}"
CONTAINER_TYPE="${CONTAINER_TYPE:-singularity}"
CONCURRENCY="${CONCURRENCY:-4}"
SSH_USER="${SSH_USER:-airflow}"
SINGULARITY_IMAGE="${SINGULARITY_IMAGE:-airflow_2.10.2.sif}"

# Ports for tunnels
POSTGRES_PORT=5432
REDIS_PORT=6379

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

################################################################################
# Cleanup function
################################################################################
cleanup() {
    log_info "Cleaning up worker processes..."
    
    # Stop Celery worker
    pkill -f "celery.*worker.*${WORKER_NAME}" 2>/dev/null || true
    
    # Close SSH tunnels
    pkill -f "ssh.*-L.*${POSTGRES_PORT}" 2>/dev/null || true
    pkill -f "ssh.*-L.*${REDIS_PORT}" 2>/dev/null || true
    
    log_info "Cleanup completed"
}

################################################################################
# Setup SSH tunnels
################################################################################
setup_tunnels() {
    log_info "Setting up SSH tunnels to master ${MASTER_IP}..."
    
    # Kill existing tunnels
    pkill -f "ssh.*-L.*${POSTGRES_PORT}" 2>/dev/null || true
    pkill -f "ssh.*-L.*${REDIS_PORT}" 2>/dev/null || true
    sleep 2
    
    # Create new tunnels
    ssh -f -N \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ServerAliveInterval=60 \
        -o ServerAliveCountMax=3 \
        -o ExitOnForwardFailure=yes \
        -L ${POSTGRES_PORT}:localhost:${POSTGRES_PORT} \
        -L ${REDIS_PORT}:localhost:${REDIS_PORT} \
        ${SSH_USER}@${MASTER_IP}
    
    if [ $? -eq 0 ]; then
        log_info "✅ SSH tunnels established successfully"
        sleep 3
        
        # Verify tunnels are working
        if ! netstat -tuln | grep -q ":${POSTGRES_PORT}"; then
            log_error "PostgreSQL tunnel not established"
            return 1
        fi
        
        if ! netstat -tuln | grep -q ":${REDIS_PORT}"; then
            log_error "Redis tunnel not established"
            return 1
        fi
        
        log_info "✅ All tunnels verified"
        return 0
    else
        log_error "Failed to establish SSH tunnels"
        return 1
    fi
}

################################################################################
# Start worker with Singularity
################################################################################
start_worker_singularity() {
    log_info "Starting Celery worker with Singularity..."
    
    # Check for Singularity image in AIRFLOW_HOME
    if [ ! -f "${AIRFLOW_HOME}/${SINGULARITY_IMAGE}" ]; then
        log_error "Singularity image not found: ${AIRFLOW_HOME}/${SINGULARITY_IMAGE}"
        log_error "Please copy the Singularity image from master server:"
        log_error "  scp master:/home/airflow/airflow/${SINGULARITY_IMAGE} ${AIRFLOW_HOME}/"
        return 1
    fi
    
    log_info "Using Singularity image: ${AIRFLOW_HOME}/${SINGULARITY_IMAGE}"
    cd "${AIRFLOW_HOME}"
    
    # Start worker using singularity exec
    nohup singularity exec \
        --bind ${AIRFLOW_HOME}:${AIRFLOW_HOME} \
        ${SINGULARITY_IMAGE} \
        celery -A airflow.providers.celery.executors.celery_executor.app worker \
        --hostname="${WORKER_NAME}" \
        --queues="${QUEUE_NAME},default" \
        --concurrency=${CONCURRENCY} \
        --loglevel=info \
        > /tmp/worker_${WORKER_NAME}.log 2>&1 &
    
    WORKER_PID=$!
    sleep 5
    
    if ps -p ${WORKER_PID} > /dev/null; then
        log_info "✅ Singularity worker started (PID: ${WORKER_PID})"
        echo ${WORKER_PID} > /tmp/worker_${WORKER_NAME}.pid
        return 0
    else
        log_error "Failed to start Singularity worker"
        log_info "Last 20 lines of worker log:"
        tail -20 /tmp/worker_${WORKER_NAME}.log
        return 1
    fi
}



################################################################################
# Start worker with Python venv
################################################################################
start_worker_venv() {
    log_info "Starting Celery worker with Python venv..."
    
    cd "${AIRFLOW_HOME}"
    
    # Activate virtual environment
    if [ -f venv/bin/activate ]; then
        source venv/bin/activate
    else
        log_error "Virtual environment not found: ${AIRFLOW_HOME}/venv"
        return 1
    fi
    
    # Start worker
    nohup celery -A airflow.providers.celery.executors.celery_executor.app worker \
        --hostname="${WORKER_NAME}" \
        --queues="${QUEUE_NAME},default" \
        --concurrency=${CONCURRENCY} \
        --loglevel=info \
        > /tmp/worker_${WORKER_NAME}.log 2>&1 &
    
    WORKER_PID=$!
    sleep 5
    
    if ps -p ${WORKER_PID} > /dev/null; then
        log_info "✅ Python venv worker started (PID: ${WORKER_PID})"
        echo ${WORKER_PID} > /tmp/worker_${WORKER_NAME}.pid
        return 0
    else
        log_error "Failed to start Python venv worker"
        tail -20 /tmp/worker_${WORKER_NAME}.log
        return 1
    fi
}

################################################################################
# Main execution
################################################################################
main() {
    log_info "=========================================="
    log_info "Airflow Remote Worker Launcher"
    log_info "=========================================="
    log_info "Worker Name: ${WORKER_NAME}"
    log_info "Queue: ${QUEUE_NAME}"
    log_info "Master IP: ${MASTER_IP}"
    log_info "Container: ${CONTAINER_TYPE}"
    log_info "Airflow Home: ${AIRFLOW_HOME}"
    log_info "=========================================="
    
    # Cleanup any existing processes
    cleanup
    
    # Setup SSH tunnels
    if ! setup_tunnels; then
        log_error "Failed to setup SSH tunnels"
        exit 1
    fi
    
    # Start worker with Singularity (default) or fallback to venv
    if [ "${CONTAINER_TYPE}" = "venv" ] || [ "${CONTAINER_TYPE}" = "python" ]; then
        # User explicitly requested venv
        if ! start_worker_venv; then
            log_error "Failed to start Python venv worker"
            cleanup
            exit 1
        fi
    else
        # Try Singularity first (default)
        if ! start_worker_singularity; then
            log_warn "Singularity start failed, falling back to Python venv..."
            if ! start_worker_venv; then
                log_error "All worker start methods failed"
                cleanup
                exit 1
            fi
        fi
    fi
    
    # Verify worker is running
    sleep 5
    if pgrep -f "celery.*worker.*${WORKER_NAME}" > /dev/null; then
        log_info "=========================================="
        log_info "✅ Worker ${WORKER_NAME} started successfully!"
        log_info "=========================================="
        log_info "Worker is now ready to accept tasks"
        log_info "Log file: /tmp/worker_${WORKER_NAME}.log"
        
        # Show last few log lines
        if [ -f /tmp/worker_${WORKER_NAME}.log ]; then
            log_info ""
            log_info "Recent log output:"
            tail -10 /tmp/worker_${WORKER_NAME}.log
        fi
        
        exit 0
    else
        log_error "Worker verification failed"
        cleanup
        exit 1
    fi
}

# Trap signals for cleanup
trap cleanup EXIT INT TERM

# Run main function
main "$@"
