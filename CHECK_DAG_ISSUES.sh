#!/bin/bash
#########################################################
# CHECK DAG ISSUES - Diagnose test_ondemand_worker
# Run on Master Server as airflow user
#########################################################

set +e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================================================"
echo "  CHECKING test_ondemand_worker DAG"
echo "========================================================================"
echo ""

#########################################################
# CHECK 1: DAG File Exists
#########################################################

echo -e "${BLUE}CHECK 1: Does DAG file exist?${NC}"
if [ -f ~/airflow/dags/test_ondemand_worker.py ]; then
    echo -e "${GREEN}✅ DAG file found: ~/airflow/dags/test_ondemand_worker.py${NC}"
else
    echo -e "${RED}❌ DAG file NOT found!${NC}"
    exit 1
fi
echo ""

#########################################################
# CHECK 2: Python Syntax Check
#########################################################

echo -e "${BLUE}CHECK 2: Python syntax check${NC}"
if python3 -m py_compile ~/airflow/dags/test_ondemand_worker.py 2>/dev/null; then
    echo -e "${GREEN}✅ No syntax errors${NC}"
else
    echo -e "${RED}❌ Syntax errors found:${NC}"
    python3 -m py_compile ~/airflow/dags/test_ondemand_worker.py
fi
echo ""

#########################################################
# CHECK 3: Can Airflow Parse DAG?
#########################################################

echo -e "${BLUE}CHECK 3: Can Airflow parse the DAG?${NC}"
PARSE_OUTPUT=$(airflow dags list 2>&1 | grep test_ondemand_worker)
if [ -n "$PARSE_OUTPUT" ]; then
    echo -e "${GREEN}✅ DAG parsed successfully${NC}"
    echo "   $PARSE_OUTPUT"
else
    echo -e "${RED}❌ DAG not in list (may have parsing errors)${NC}"
    echo ""
    echo "Checking for import errors..."
    airflow dags list-import-errors
fi
echo ""

#########################################################
# CHECK 4: Plugin Import Issue
#########################################################

echo -e "${BLUE}CHECK 4: Checking custom operator imports${NC}"
if grep -q "from operators.remote_worker_operator import" ~/airflow/dags/test_ondemand_worker.py; then
    echo -e "${YELLOW}⚠️  DAG imports from 'operators.remote_worker_operator'${NC}"
    
    # Check if plugin exists
    if [ -f ~/airflow/plugins/operators/remote_worker_operator.py ]; then
        echo -e "${GREEN}✅ Plugin file exists${NC}"
        
        # Check if __init__.py exists
        if [ -f ~/airflow/plugins/operators/__init__.py ]; then
            echo -e "${GREEN}✅ __init__.py exists${NC}"
        else
            echo -e "${RED}❌ Missing: ~/airflow/plugins/operators/__init__.py${NC}"
            echo -e "${YELLOW}   Creating it now...${NC}"
            touch ~/airflow/plugins/operators/__init__.py
            echo -e "${GREEN}✅ Created${NC}"
        fi
        
        if [ -f ~/airflow/plugins/__init__.py ]; then
            echo -e "${GREEN}✅ plugins/__init__.py exists${NC}"
        else
            echo -e "${RED}❌ Missing: ~/airflow/plugins/__init__.py${NC}"
            echo -e "${YELLOW}   Creating it now...${NC}"
            touch ~/airflow/plugins/__init__.py
            echo -e "${GREEN}✅ Created${NC}"
        fi
    else
        echo -e "${RED}❌ Plugin file NOT found!${NC}"
        echo -e "${RED}   Expected: ~/airflow/plugins/operators/remote_worker_operator.py${NC}"
    fi
else
    echo -e "${GREEN}✅ No custom operator imports${NC}"
fi
echo ""

#########################################################
# CHECK 5: SSH Connection Required
#########################################################

echo -e "${BLUE}CHECK 5: Checking required Airflow Connection${NC}"
if grep -q "ssh_conn_id='remote_worker_ssh'" ~/airflow/dags/test_ondemand_worker.py; then
    echo -e "${YELLOW}⚠️  DAG requires 'remote_worker_ssh' connection${NC}"
    
    # Check if connection exists
    CONN_CHECK=$(airflow connections get remote_worker_ssh 2>&1)
    if echo "$CONN_CHECK" | grep -q "Connection not found"; then
        echo -e "${RED}❌ Connection 'remote_worker_ssh' NOT found!${NC}"
        echo -e "${YELLOW}   This will cause DAG to fail!${NC}"
        echo ""
        echo "Create it with:"
        echo "  airflow connections add 'remote_worker_ssh' \\"
        echo "    --conn-type 'ssh' \\"
        echo "    --conn-host 'remotedimpal-2' \\"
        echo "    --conn-login 'airflow' \\"
        echo "    --conn-extra '{\"key_file\": \"/home/airflow/.ssh/id_rsa\"}'"
    else
        echo -e "${GREEN}✅ Connection 'remote_worker_ssh' exists${NC}"
    fi
else
    echo -e "${GREEN}✅ No SSH connection required${NC}"
fi
echo ""

#########################################################
# CHECK 6: Executor Compatibility
#########################################################

echo -e "${BLUE}CHECK 6: Executor compatibility${NC}"
EXECUTOR=$(grep "^executor = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
echo "   Current executor: $EXECUTOR"

if grep -q "queue='worker1'" ~/airflow/dags/test_ondemand_worker.py; then
    echo -e "${YELLOW}⚠️  DAG uses queue='worker1' (requires CeleryExecutor)${NC}"
    
    if [ "$EXECUTOR" != "CeleryExecutor" ]; then
        echo -e "${RED}❌ INCOMPATIBLE! Queue assignment doesn't work with $EXECUTOR${NC}"
        echo -e "${YELLOW}   Queue='worker1' will be IGNORED${NC}"
        echo -e "${YELLOW}   Task will run on Master with LocalExecutor${NC}"
    else
        echo -e "${GREEN}✅ CeleryExecutor configured (queue will work)${NC}"
    fi
else
    echo -e "${GREEN}✅ No queue assignment${NC}"
fi
echo ""

#########################################################
# CHECK 7: DAG Run Status
#########################################################

echo -e "${BLUE}CHECK 7: Current DAG run status${NC}"
DAG_RUNS=$(airflow dags list-runs -d test_ondemand_worker 2>/dev/null | tail -n +4 | head -5)
if [ -n "$DAG_RUNS" ]; then
    echo "Recent runs:"
    echo "$DAG_RUNS"
else
    echo -e "${YELLOW}⚠️  No DAG runs found${NC}"
    echo "   DAG has not been triggered yet"
fi
echo ""

#########################################################
# CHECK 8: Log Directory Structure
#########################################################

echo -e "${BLUE}CHECK 8: Checking log directory${NC}"
if [ -d ~/airflow/logs ]; then
    echo -e "${GREEN}✅ Logs directory exists${NC}"
    
    # Check for DAG-specific logs
    if [ -d ~/airflow/logs/dag_id=test_ondemand_worker ]; then
        echo -e "${GREEN}✅ DAG logs directory exists${NC}"
        echo "   Location: ~/airflow/logs/dag_id=test_ondemand_worker/"
        
        # List log files
        LOG_COUNT=$(find ~/airflow/logs/dag_id=test_ondemand_worker -name "*.log" 2>/dev/null | wc -l)
        echo "   Found $LOG_COUNT log files"
        
        if [ $LOG_COUNT -gt 0 ]; then
            echo ""
            echo "Latest logs:"
            find ~/airflow/logs/dag_id=test_ondemand_worker -name "*.log" -type f -printf '%T@ %p\n' 2>/dev/null | \
                sort -rn | head -5 | cut -d' ' -f2-
        fi
    else
        echo -e "${YELLOW}⚠️  No DAG-specific logs yet${NC}"
        echo "   (Logs appear after DAG runs)"
    fi
    
    # Check scheduler logs
    if [ -d ~/airflow/logs/scheduler ]; then
        echo ""
        echo "Scheduler logs:"
        SCHEDULER_LOGS=$(find ~/airflow/logs/scheduler -name "*.log" -type f -printf '%T@ %p\n' 2>/dev/null | \
            sort -rn | head -3 | cut -d' ' -f2-)
        if [ -n "$SCHEDULER_LOGS" ]; then
            echo "$SCHEDULER_LOGS"
        fi
    fi
else
    echo -e "${RED}❌ Logs directory NOT found!${NC}"
fi
echo ""

#########################################################
# CHECK 9: Parsing Errors in Scheduler Logs
#########################################################

echo -e "${BLUE}CHECK 9: Checking scheduler logs for DAG errors${NC}"
if [ -d ~/airflow/logs/scheduler/latest ]; then
    echo "Searching for 'test_ondemand_worker' errors..."
    
    ERRORS=$(grep -i "error\|exception\|failed" ~/airflow/logs/scheduler/latest/*.log 2>/dev/null | \
        grep -i "test_ondemand_worker" | tail -5)
    
    if [ -n "$ERRORS" ]; then
        echo -e "${RED}❌ Errors found:${NC}"
        echo "$ERRORS"
    else
        echo -e "${GREEN}✅ No errors found in recent scheduler logs${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  No scheduler logs found${NC}"
fi
echo ""

#########################################################
# SUMMARY
#########################################################

echo "========================================================================"
echo -e "${BLUE}DIAGNOSIS SUMMARY${NC}"
echo "========================================================================"
echo ""

# Count issues
ISSUES=0

# Check critical issues
if ! airflow dags list 2>/dev/null | grep -q test_ondemand_worker; then
    echo -e "${RED}❌ CRITICAL: DAG not parsed by Airflow${NC}"
    ISSUES=$((ISSUES + 1))
fi

if ! [ -f ~/airflow/plugins/operators/__init__.py ]; then
    echo -e "${YELLOW}⚠️  WARNING: Missing __init__.py in plugins/operators/${NC}"
    echo "   (May cause import errors)"
    ISSUES=$((ISSUES + 1))
fi

if ! airflow connections get remote_worker_ssh &>/dev/null; then
    echo -e "${YELLOW}⚠️  WARNING: Missing 'remote_worker_ssh' connection${NC}"
    echo "   (Required by custom operators)"
    ISSUES=$((ISSUES + 1))
fi

EXECUTOR=$(grep "^executor = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
if [ "$EXECUTOR" != "CeleryExecutor" ] && grep -q "queue='worker1'" ~/airflow/dags/test_ondemand_worker.py; then
    echo -e "${YELLOW}⚠️  WARNING: Queue assignment won't work with $EXECUTOR${NC}"
    echo "   (Tasks will run on Master, not remote)"
    ISSUES=$((ISSUES + 1))
fi

if [ $ISSUES -eq 0 ]; then
    echo -e "${GREEN}✅ No critical issues found!${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Trigger DAG: airflow dags trigger test_ondemand_worker"
    echo "  2. Check logs: bash VIEW_DAG_LOGS.sh"
else
    echo ""
    echo -e "${YELLOW}Found $ISSUES potential issue(s)${NC}"
    echo ""
    echo "Recommended fixes:"
    echo "  1. Use simplified test DAG: bash CREATE_SIMPLE_TEST_DAG.sh"
    echo "  2. Or fix issues listed above"
fi

echo ""
echo "========================================================================"
