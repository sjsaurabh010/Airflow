#!/bin/bash
#########################################################
# STOP CURRENT DAG AND RESTART WITH CELERYEXECUTOR
# Run on Master Server as airflow user
#########################################################

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

DAG_ID="test_ondemand_worker"

echo "========================================================================"
echo "  STOP AND RESTART DAG WITH CELERYEXECUTOR"
echo "========================================================================"
echo ""

#########################################################
# Step 1: Check current status
#########################################################

echo -e "${BLUE}Step 1: Checking current status...${NC}"
EXECUTOR=$(grep "^executor = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
echo "  Current executor: $EXECUTOR"

# Check if Redis is running
if redis-cli ping &>/dev/null; then
    echo -e "  ${GREEN}✓ Redis running (no password)${NC}"
    REDIS_OK=1
else
    # Try with password from config
    BROKER_URL=$(grep "^broker_url = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
    if echo "$BROKER_URL" | grep -q "redis"; then
        REDIS_PASSWORD=$(echo "$BROKER_URL" | sed -n 's/.*:\/\/:\([^@]*\)@.*/\1/p')
        if [ -n "$REDIS_PASSWORD" ] && redis-cli -a "$REDIS_PASSWORD" ping &>/dev/null 2>&1; then
            echo -e "  ${GREEN}✓ Redis running (with password)${NC}"
            REDIS_OK=1
        else
            echo -e "  ${RED}✗ Redis not responding${NC}"
            REDIS_OK=0
        fi
    else
        echo -e "  ${YELLOW}⚠ Redis not configured${NC}"
        REDIS_OK=0
    fi
fi

# Check PostgreSQL
DB_CONN=$(grep "^sql_alchemy_conn = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
if echo "$DB_CONN" | grep -q "postgresql"; then
    PG_PASSWORD=$(echo "$DB_CONN" | sed -n 's/.*:\/\/airflow:\([^@]*\)@.*/\1/p')
    export PGPASSWORD="$PG_PASSWORD"
    if psql -h localhost -U airflow -d airflow -c "SELECT 1;" &>/dev/null 2>&1; then
        echo -e "  ${GREEN}✓ PostgreSQL running${NC}"
        PG_OK=1
    else
        echo -e "  ${RED}✗ PostgreSQL not responding${NC}"
        PG_OK=0
    fi
else
    echo -e "  ${YELLOW}⚠ PostgreSQL not configured${NC}"
    PG_OK=0
fi

echo ""

#########################################################
# Step 2: Stop current running DAG
#########################################################

echo -e "${BLUE}Step 2: Stopping current DAG runs...${NC}"

# Check if DAG is running
RUNNING_RUNS=$(airflow dags list-runs -d "$DAG_ID" --state running 2>/dev/null | tail -n +4)

if [ -n "$RUNNING_RUNS" ]; then
    echo "Found running DAG instances:"
    echo "$RUNNING_RUNS"
    echo ""
    
    read -p "Stop these runs? (yes/no): " CONFIRM_STOP
    if [ "$CONFIRM_STOP" == "yes" ]; then
        # Get run IDs and mark them as failed
        echo "$RUNNING_RUNS" | while read line; do
            RUN_ID=$(echo "$line" | awk '{print $3}')
            if [ -n "$RUN_ID" ]; then
                echo "  Stopping run: $RUN_ID"
                airflow dags state "$DAG_ID" "$RUN_ID" 2>/dev/null || true
            fi
        done
        echo -e "${GREEN}✓ Running instances stopped${NC}"
    else
        echo "Keeping current runs active"
    fi
else
    echo -e "${GREEN}✓ No running instances found${NC}"
fi

echo ""

#########################################################
# Step 3: Clear failed runs (optional)
#########################################################

echo -e "${BLUE}Step 3: Clear failed runs?${NC}"

FAILED_RUNS=$(airflow dags list-runs -d "$DAG_ID" --state failed 2>/dev/null | tail -n +4)

if [ -n "$FAILED_RUNS" ]; then
    echo "Found failed runs:"
    echo "$FAILED_RUNS" | head -3
    echo ""
    
    read -p "Clear all failed runs? (yes/no): " CONFIRM_CLEAR
    if [ "$CONFIRM_CLEAR" == "yes" ]; then
        airflow dags delete "$DAG_ID" --yes 2>/dev/null || true
        echo -e "${GREEN}✓ Failed runs cleared${NC}"
    else
        echo "Keeping failed runs for reference"
    fi
else
    echo -e "${GREEN}✓ No failed runs to clear${NC}"
fi

echo ""

#########################################################
# Step 4: Switch to CeleryExecutor (if needed)
#########################################################

if [ "$EXECUTOR" != "CeleryExecutor" ] && [ "$REDIS_OK" == "1" ] && [ "$PG_OK" == "1" ]; then
    echo -e "${BLUE}Step 4: Switching to CeleryExecutor...${NC}"
    echo "  You have Redis and PostgreSQL available"
    echo ""
    
    read -p "Switch to CeleryExecutor? (yes/no): " CONFIRM_SWITCH
    if [ "$CONFIRM_SWITCH" == "yes" ]; then
        # Backup config
        cp ~/airflow/airflow.cfg ~/airflow/airflow.cfg.backup_$(date +%Y%m%d_%H%M%S)
        echo "  ✓ Config backed up"
        
        # Change executor
        sed -i "s/^executor = .*/executor = CeleryExecutor/" ~/airflow/airflow.cfg
        echo "  ✓ Changed to CeleryExecutor"
        
        # Verify broker_url is set
        BROKER_URL=$(grep "^broker_url = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
        if [ -z "$BROKER_URL" ] || [ "$BROKER_URL" == "redis://redis:6379/0" ]; then
            echo ""
            echo "  Redis password needed for broker_url"
            read -p "  Enter Redis password (or press Enter if no password): " REDIS_PASS
            if [ -n "$REDIS_PASS" ]; then
                sed -i "s|^broker_url = .*|broker_url = redis://:${REDIS_PASS}@localhost:6379/0|" ~/airflow/airflow.cfg
            else
                sed -i "s|^broker_url = .*|broker_url = redis://localhost:6379/0|" ~/airflow/airflow.cfg
            fi
            echo "  ✓ broker_url configured"
        fi
        
        # Verify result_backend is set
        RESULT_BACKEND=$(grep "^result_backend = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
        if [ -z "$RESULT_BACKEND" ]; then
            sed -i "s|^result_backend = .*|result_backend = db+postgresql://airflow:${PG_PASSWORD}@localhost/airflow|" ~/airflow/airflow.cfg
            echo "  ✓ result_backend configured"
        fi
        
        echo -e "${GREEN}✓ Switched to CeleryExecutor${NC}"
    else
        echo "Keeping current executor: $EXECUTOR"
    fi
else
    echo -e "${BLUE}Step 4: Executor check${NC}"
    if [ "$EXECUTOR" == "CeleryExecutor" ]; then
        echo -e "${GREEN}✓ Already using CeleryExecutor${NC}"
    else
        echo -e "${YELLOW}⚠ Cannot switch to CeleryExecutor (Redis or PostgreSQL not available)${NC}"
        echo "  Current executor: $EXECUTOR"
    fi
fi

echo ""

#########################################################
# Step 5: Restart Scheduler
#########################################################

echo -e "${BLUE}Step 5: Restarting Scheduler...${NC}"

# Stop scheduler
if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
    echo "  Stopping scheduler..."
    pkill -f "airflow scheduler"
    sleep 3
fi

# Start scheduler
echo "  Starting scheduler..."
airflow scheduler -D
sleep 5

# Verify
if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
    echo -e "${GREEN}✓ Scheduler restarted${NC}"
    
    # Show new executor
    NEW_EXECUTOR=$(grep "^executor = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
    echo "  New executor: $NEW_EXECUTOR"
else
    echo -e "${RED}✗ Scheduler failed to start!${NC}"
    echo "  Check logs: tail -f ~/airflow/logs/scheduler/latest/*.log"
    exit 1
fi

echo ""

#########################################################
# Step 6: Start local Celery worker (optional, for testing)
#########################################################

if [ "$NEW_EXECUTOR" == "CeleryExecutor" ] || [ "$EXECUTOR" == "CeleryExecutor" ]; then
    echo -e "${BLUE}Step 6: Start local Celery worker for testing?${NC}"
    echo "  This runs a worker ON MASTER for testing"
    echo "  (Later you'll use remote workers)"
    echo ""
    
    read -p "Start local Celery worker? (yes/no): " START_WORKER
    if [ "$START_WORKER" == "yes" ]; then
        echo ""
        echo "Starting Celery worker in background..."
        echo "  (Logs: ~/airflow/logs/celery-worker.log)"
        
        nohup airflow celery worker > ~/airflow/logs/celery-worker.log 2>&1 &
        WORKER_PID=$!
        
        sleep 5
        
        if ps -p $WORKER_PID > /dev/null; then
            echo -e "${GREEN}✓ Celery worker started (PID: $WORKER_PID)${NC}"
            echo "  To stop: kill $WORKER_PID"
            echo "$WORKER_PID" > ~/airflow/celery-worker.pid
        else
            echo -e "${RED}✗ Worker failed to start${NC}"
            echo "  Check logs: tail -f ~/airflow/logs/celery-worker.log"
        fi
    else
        echo "Skipped. You'll need a worker to execute tasks!"
    fi
else
    echo -e "${BLUE}Step 6: Celery worker not needed (using LocalExecutor)${NC}"
fi

echo ""

#########################################################
# Summary
#########################################################

echo "========================================================================"
echo -e "${GREEN}SETUP COMPLETE!${NC}"
echo "========================================================================"
echo ""

FINAL_EXECUTOR=$(grep "^executor = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
echo "Configuration:"
echo "  Executor: $FINAL_EXECUTOR"
echo "  Database: PostgreSQL"
if [ "$FINAL_EXECUTOR" == "CeleryExecutor" ]; then
    echo "  Broker: Redis"
    
    # Check if worker is running
    if ps aux | grep -v grep | grep "airflow celery worker" > /dev/null; then
        echo "  Worker: Running locally"
    else
        echo "  Worker: Not running (start one to execute tasks!)"
    fi
fi

echo ""
echo "NEXT STEPS:"
echo ""
echo "1. Trigger DAG:"
echo "   airflow dags trigger $DAG_ID"
echo ""
echo "2. Monitor execution:"
echo "   watch -n 5 'airflow dags list-runs -d $DAG_ID'"
echo ""
echo "3. View logs:"
echo "   bash VIEW_DAG_LOGS.sh"
echo ""
echo "4. Or view logs in real-time:"
echo "   tail -f ~/airflow/logs/dag_id=$DAG_ID/run_id=*/task_id=*/*.log"
echo ""

if [ "$FINAL_EXECUTOR" == "CeleryExecutor" ]; then
    echo "5. Check Celery worker status:"
    echo "   celery -A airflow.providers.celery.executors.celery_executor.app inspect active"
    echo ""
    echo "6. View worker logs:"
    echo "   tail -f ~/airflow/logs/celery-worker.log"
    echo ""
fi

echo "========================================================================"
