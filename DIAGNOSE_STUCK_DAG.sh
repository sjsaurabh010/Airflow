#!/bin/bash
#########################################################
# DIAGNOSTIC SCRIPT - Why is DAG Stuck in Queued State?
# Run on Master Server as airflow user
#########################################################

set +e  # Don't exit on errors (we want to see all checks)

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================================================"
echo "  DIAGNOSTIC: Why is test_ondemand_worker DAG stuck?"
echo "========================================================================"
echo ""

#########################################################
# CHECK 1: Scheduler Status
#########################################################

echo -e "${BLUE}CHECK 1: Is Scheduler Running?${NC}"
if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
    echo -e "${GREEN}✅ Scheduler process is running${NC}"
    ps aux | grep -v grep | grep "airflow scheduler" | head -1
else
    echo -e "${RED}❌ PROBLEM: Scheduler is NOT running!${NC}"
    echo "   Solution: Start scheduler with: airflow scheduler -D"
fi
echo ""

#########################################################
# CHECK 2: Webserver Status
#########################################################

echo -e "${BLUE}CHECK 2: Is Webserver Running?${NC}"
if ps aux | grep -v grep | grep "airflow webserver" > /dev/null; then
    echo -e "${GREEN}✅ Webserver process is running${NC}"
    ps aux | grep -v grep | grep "airflow webserver" | head -1
else
    echo -e "${YELLOW}⚠️  Webserver is NOT running (not critical for task execution)${NC}"
fi
echo ""

#########################################################
# CHECK 3: Executor Configuration
#########################################################

echo -e "${BLUE}CHECK 3: Which Executor is Configured?${NC}"
EXECUTOR=$(grep "^executor = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
echo "   Current executor: $EXECUTOR"

if [ "$EXECUTOR" == "CeleryExecutor" ]; then
    echo -e "${YELLOW}⚠️  CeleryExecutor configured - Requires PostgreSQL + Redis + Celery Worker${NC}"
elif [ "$EXECUTOR" == "LocalExecutor" ]; then
    echo -e "${GREEN}✅ LocalExecutor - Tasks run on same machine${NC}"
elif [ "$EXECUTOR" == "SequentialExecutor" ]; then
    echo -e "${YELLOW}⚠️  SequentialExecutor - Tasks run one at a time${NC}"
fi
echo ""

#########################################################
# CHECK 4: Database Configuration
#########################################################

echo -e "${BLUE}CHECK 4: Which Database is Configured?${NC}"
SQL_CONN=$(grep "^sql_alchemy_conn = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
echo "   Database: $SQL_CONN"

if echo "$SQL_CONN" | grep -q "postgresql"; then
    echo -e "${GREEN}✅ PostgreSQL configured${NC}"
    
    # Test PostgreSQL connection
    echo "   Testing PostgreSQL connection..."
    if command -v psql &> /dev/null; then
        # Extract password from connection string
        PG_PASSWORD=$(echo "$SQL_CONN" | sed -n 's/.*:\/\/airflow:\([^@]*\)@.*/\1/p')
        export PGPASSWORD="$PG_PASSWORD"
        
        if psql -h localhost -U airflow -d airflow -c "SELECT 1;" &>/dev/null; then
            echo -e "   ${GREEN}✅ PostgreSQL connection successful${NC}"
        else
            echo -e "   ${RED}❌ PROBLEM: PostgreSQL connection FAILED!${NC}"
            echo -e "   ${RED}   This is likely why tasks are stuck!${NC}"
        fi
    else
        echo -e "   ${YELLOW}⚠️  'psql' command not available, can't test connection${NC}"
    fi
    
elif echo "$SQL_CONN" | grep -q "sqlite"; then
    echo -e "${RED}❌ PROBLEM: SQLite configured (not compatible with CeleryExecutor!)${NC}"
    echo -e "   ${YELLOW}SQLite can only work with SequentialExecutor or LocalExecutor${NC}"
    echo -e "   ${YELLOW}You MUST use PostgreSQL with CeleryExecutor${NC}"
else
    echo -e "${YELLOW}⚠️  Unknown database type${NC}"
fi
echo ""

#########################################################
# CHECK 5: Redis Configuration (if CeleryExecutor)
#########################################################

if [ "$EXECUTOR" == "CeleryExecutor" ]; then
    echo -e "${BLUE}CHECK 5: Is Redis Configured and Running?${NC}"
    
    BROKER_URL=$(grep "^broker_url = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
    echo "   Broker URL: $BROKER_URL"
    
    if echo "$BROKER_URL" | grep -q "redis"; then
        echo -e "${GREEN}✅ Redis configured as broker${NC}"
        
        # Test Redis connection
        echo "   Testing Redis connection..."
        if command -v redis-cli &> /dev/null; then
            # Extract password
            REDIS_PASSWORD=$(echo "$BROKER_URL" | sed -n 's/.*:\/\/:\([^@]*\)@.*/\1/p')
            
            if [ -n "$REDIS_PASSWORD" ]; then
                if redis-cli -a "$REDIS_PASSWORD" ping 2>/dev/null | grep -q "PONG"; then
                    echo -e "   ${GREEN}✅ Redis connection successful${NC}"
                else
                    echo -e "   ${RED}❌ PROBLEM: Redis connection FAILED!${NC}"
                    echo -e "   ${RED}   This is likely why tasks are stuck!${NC}"
                fi
            else
                if redis-cli ping 2>/dev/null | grep -q "PONG"; then
                    echo -e "   ${GREEN}✅ Redis connection successful (no password)${NC}"
                else
                    echo -e "   ${RED}❌ PROBLEM: Redis connection FAILED!${NC}"
                fi
            fi
        else
            echo -e "   ${YELLOW}⚠️  'redis-cli' command not available${NC}"
        fi
    else
        echo -e "${RED}❌ PROBLEM: Redis not configured as broker!${NC}"
    fi
    echo ""
fi

#########################################################
# CHECK 6: Celery Worker Status (if CeleryExecutor)
#########################################################

if [ "$EXECUTOR" == "CeleryExecutor" ]; then
    echo -e "${BLUE}CHECK 6: Is Celery Worker Running?${NC}"
    
    # Check for Celery worker process
    if ps aux | grep -v grep | grep "celery.*worker" > /dev/null; then
        echo -e "${GREEN}✅ Celery worker process found${NC}"
        ps aux | grep -v grep | grep "celery.*worker"
    else
        echo -e "${RED}❌ PROBLEM: NO Celery worker running!${NC}"
        echo -e "   ${YELLOW}This is why tasks are stuck in 'queued' state!${NC}"
        echo -e "   ${YELLOW}With CeleryExecutor, you NEED a Celery worker to execute tasks${NC}"
    fi
    
    # Try to inspect workers via Celery
    echo ""
    echo "   Checking registered Celery workers..."
    if command -v celery &> /dev/null; then
        WORKERS_OUTPUT=$(celery -A airflow.providers.celery.executors.celery_executor.app inspect active 2>&1)
        
        if echo "$WORKERS_OUTPUT" | grep -q "Error"; then
            echo -e "   ${RED}❌ Cannot connect to Celery (Redis/broker issue)${NC}"
            echo "   Output: $WORKERS_OUTPUT"
        elif echo "$WORKERS_OUTPUT" | grep -q "@"; then
            echo -e "   ${GREEN}✅ Active workers found:${NC}"
            echo "$WORKERS_OUTPUT" | grep "@"
        else
            echo -e "   ${RED}❌ NO registered Celery workers${NC}"
        fi
    else
        echo -e "   ${YELLOW}⚠️  'celery' command not available${NC}"
    fi
    echo ""
fi

#########################################################
# CHECK 7: DAG Status
#########################################################

echo -e "${BLUE}CHECK 7: Current DAG Run Status${NC}"
if command -v airflow &> /dev/null; then
    airflow dags list-runs -d test_ondemand_worker --state queued 2>/dev/null || true
    airflow dags list-runs -d test_ondemand_worker --state running 2>/dev/null || true
    airflow dags list-runs -d test_ondemand_worker --state failed 2>/dev/null || true
else
    echo -e "${YELLOW}⚠️  'airflow' command not available${NC}"
fi
echo ""

#########################################################
# CHECK 8: Scheduler Logs (Recent Errors)
#########################################################

echo -e "${BLUE}CHECK 8: Recent Scheduler Logs (Last 20 Lines)${NC}"
if [ -f ~/airflow/logs/scheduler/latest/test_ondemand_worker.py.log ]; then
    tail -20 ~/airflow/logs/scheduler/latest/test_ondemand_worker.py.log
elif ls ~/airflow/logs/scheduler/latest/*.log &>/dev/null; then
    echo "Recent scheduler activity:"
    tail -20 $(ls -t ~/airflow/logs/scheduler/latest/*.log | head -1)
else
    echo -e "${YELLOW}⚠️  No scheduler logs found${NC}"
fi
echo ""

#########################################################
# DIAGNOSIS SUMMARY
#########################################################

echo "========================================================================"
echo -e "${BLUE}DIAGNOSIS SUMMARY${NC}"
echo "========================================================================"
echo ""

if [ "$EXECUTOR" == "CeleryExecutor" ]; then
    echo -e "${YELLOW}You are using CeleryExecutor. This requires:${NC}"
    echo "   1. ✓ Scheduler running"
    echo "   2. ? PostgreSQL database (not SQLite)"
    echo "   3. ? Redis message broker"
    echo "   4. ? Celery worker process"
    echo ""
    echo -e "${YELLOW}Most likely problem:${NC}"
    echo -e "${RED}   ❌ Phase 1 is INCOMPLETE!${NC}"
    echo -e "${RED}   ❌ You configured CeleryExecutor but didn't install PostgreSQL + Redis${NC}"
    echo ""
    echo -e "${BLUE}SOLUTION OPTIONS:${NC}"
    echo ""
    echo -e "${GREEN}Option 1 (QUICK FIX - For Testing Only):${NC}"
    echo "   Switch back to LocalExecutor temporarily:"
    echo "   1. Edit ~/airflow/airflow.cfg"
    echo "   2. Change: executor = CeleryExecutor"
    echo "   3. To:     executor = LocalExecutor"
    echo "   4. Restart scheduler: pkill -f 'airflow scheduler' && airflow scheduler -D"
    echo "   5. Retry DAG trigger"
    echo ""
    echo -e "${GREEN}Option 2 (PROPER FIX - For Production):${NC}"
    echo "   Complete Phase 1 properly:"
    echo "   1. Install PostgreSQL (see PHASE1_MASTER_SETUP_COMMANDS.HTML)"
    echo "   2. Install Redis (see PHASE1_MASTER_SETUP_COMMANDS.HTML)"
    echo "   3. Update airflow.cfg with correct connection strings"
    echo "   4. Run: airflow db migrate"
    echo "   5. Start Celery worker (for local testing)"
    echo "   6. Then proceed to Phase 2 (remote workers)"
else
    echo "Your executor is: $EXECUTOR"
    echo "Check logs above for specific issues."
fi

echo ""
echo "========================================================================"
echo "For detailed Phase 1 setup, open:"
echo "   PHASE1_MASTER_SETUP_COMMANDS.HTML"
echo "========================================================================"
