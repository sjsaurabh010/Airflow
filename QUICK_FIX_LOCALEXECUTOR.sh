#!/bin/bash
#########################################################
# QUICK FIX: Switch to LocalExecutor (Temporary)
# Run on Master Server as airflow user
# This allows testing DAGs without PostgreSQL + Redis
#########################################################

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================================================"
echo "  QUICK FIX: Switch to LocalExecutor"
echo "========================================================================"
echo ""

echo -e "${YELLOW}WARNING: This is a TEMPORARY fix for testing only!${NC}"
echo -e "${YELLOW}For production, you should complete Phase 1 with PostgreSQL + Redis.${NC}"
echo ""
read -p "Continue with LocalExecutor switch? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Cancelled."
    exit 0
fi

echo ""
echo -e "${BLUE}Step 1: Backup current configuration${NC}"
cp ~/airflow/airflow.cfg ~/airflow/airflow.cfg.backup_$(date +%Y%m%d_%H%M%S)
echo -e "${GREEN}✅ Backed up to: ~/airflow/airflow.cfg.backup_$(date +%Y%m%d_%H%M%S)${NC}"

echo ""
echo -e "${BLUE}Step 2: Change executor to LocalExecutor${NC}"
sed -i 's/^executor = CeleryExecutor/executor = LocalExecutor/' ~/airflow/airflow.cfg
sed -i 's/^executor = SequentialExecutor/executor = LocalExecutor/' ~/airflow/airflow.cfg

# Verify change
NEW_EXECUTOR=$(grep "^executor = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
echo "   New executor: $NEW_EXECUTOR"

if [ "$NEW_EXECUTOR" == "LocalExecutor" ]; then
    echo -e "${GREEN}✅ Executor changed to LocalExecutor${NC}"
else
    echo -e "${RED}❌ Failed to change executor!${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}Step 3: Stop scheduler (if running)${NC}"
if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
    echo "   Stopping scheduler..."
    pkill -f "airflow scheduler" || true
    sleep 3
    echo -e "${GREEN}✅ Scheduler stopped${NC}"
else
    echo "   Scheduler not running"
fi

echo ""
echo -e "${BLUE}Step 4: Restart scheduler with new configuration${NC}"
echo "   Starting scheduler in background..."
airflow scheduler -D

# Wait for scheduler to start
sleep 5

if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
    echo -e "${GREEN}✅ Scheduler restarted with LocalExecutor${NC}"
else
    echo -e "${RED}❌ Scheduler failed to start!${NC}"
    echo "   Check logs: tail -f ~/airflow/logs/scheduler/latest/*.log"
    exit 1
fi

echo ""
echo -e "${BLUE}Step 5: Verify configuration${NC}"
echo "   Checking Airflow DB connection..."
if airflow db check &>/dev/null; then
    echo -e "${GREEN}✅ Database connection OK${NC}"
else
    echo -e "${YELLOW}⚠️  Database check returned warnings (may be OK)${NC}"
fi

echo ""
echo "========================================================================"
echo -e "${GREEN}✅ QUICK FIX COMPLETE!${NC}"
echo "========================================================================"
echo ""
echo "You can now test your DAG:"
echo "   airflow dags trigger test_ondemand_worker"
echo ""
echo "Monitor progress:"
echo "   airflow dags list-runs -d test_ondemand_worker"
echo ""
echo "View logs:"
echo "   tail -f ~/airflow/logs/dag_id=test_ondemand_worker/run_id=*/task_id=start_remote_worker/*.log"
echo ""
echo -e "${YELLOW}NOTE: LocalExecutor runs tasks on THIS machine (Master).${NC}"
echo -e "${YELLOW}For remote execution, complete Phase 1 + Phase 2 properly.${NC}"
echo ""
echo "To revert to CeleryExecutor:"
echo "   1. Install PostgreSQL + Redis (see PHASE1_MASTER_SETUP_COMMANDS.HTML)"
echo "   2. Restore backup: cp ~/airflow/airflow.cfg.backup_* ~/airflow/airflow.cfg"
echo "   3. Restart scheduler"
echo "========================================================================"
