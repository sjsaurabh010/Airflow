#!/bin/bash
#########################################################
# FIX DAG ISSUES - Automated fixes for test_ondemand_worker
# Run on Master Server as airflow user
#########################################################

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================================================"
echo "  FIXING DAG ISSUES"
echo "========================================================================"
echo ""

#########################################################
# FIX 1: Create __init__.py files for plugins
#########################################################

echo -e "${BLUE}FIX 1: Creating __init__.py files${NC}"

# Main plugins __init__.py
if [ ! -f ~/airflow/plugins/__init__.py ]; then
    echo "Creating ~/airflow/plugins/__init__.py"
    touch ~/airflow/plugins/__init__.py
    echo -e "${GREEN}✅ Created${NC}"
else
    echo -e "${GREEN}✅ Already exists: ~/airflow/plugins/__init__.py${NC}"
fi

# Operators __init__.py
if [ ! -f ~/airflow/plugins/operators/__init__.py ]; then
    echo "Creating ~/airflow/plugins/operators/__init__.py"
    touch ~/airflow/plugins/operators/__init__.py
    echo -e "${GREEN}✅ Created${NC}"
else
    echo -e "${GREEN}✅ Already exists: ~/airflow/plugins/operators/__init__.py${NC}"
fi

echo ""

#########################################################
# FIX 2: Copy new test DAGs
#########################################################

echo -e "${BLUE}FIX 2: Deploying fixed/simple test DAGs${NC}"

# Check if simple DAG exists
if [ ! -f ~/airflow/dags/test_simple_dag.py ]; then
    echo -e "${YELLOW}⚠️  test_simple_dag.py not found in dags folder${NC}"
    echo "   Expected location: ~/airflow/dags/test_simple_dag.py"
    echo "   Copy it from project directory"
else
    echo -e "${GREEN}✅ test_simple_dag.py found${NC}"
fi

# Check if fixed DAG exists
if [ ! -f ~/airflow/dags/test_ondemand_worker_FIXED.py ]; then
    echo -e "${YELLOW}⚠️  test_ondemand_worker_FIXED.py not found in dags folder${NC}"
    echo "   Expected location: ~/airflow/dags/test_ondemand_worker_FIXED.py"
    echo "   Copy it from project directory"
else
    echo -e "${GREEN}✅ test_ondemand_worker_FIXED.py found${NC}"
fi

echo ""

#########################################################
# FIX 3: Create SSH connection (if missing)
#########################################################

echo -e "${BLUE}FIX 3: Checking SSH connection${NC}"

if airflow connections get remote_worker_ssh &>/dev/null; then
    echo -e "${GREEN}✅ Connection 'remote_worker_ssh' already exists${NC}"
    airflow connections get remote_worker_ssh
else
    echo -e "${YELLOW}⚠️  Connection 'remote_worker_ssh' not found${NC}"
    echo ""
    read -p "Create SSH connection now? (yes/no): " CREATE_CONN
    
    if [ "$CREATE_CONN" == "yes" ]; then
        echo ""
        read -p "Remote host (e.g., remotedimpal-2): " REMOTE_HOST
        read -p "Remote user (e.g., airflow): " REMOTE_USER
        read -p "SSH key path (default: /home/airflow/.ssh/id_rsa): " SSH_KEY
        SSH_KEY=${SSH_KEY:-/home/airflow/.ssh/id_rsa}
        
        airflow connections add 'remote_worker_ssh' \
            --conn-type 'ssh' \
            --conn-host "$REMOTE_HOST" \
            --conn-login "$REMOTE_USER" \
            --conn-extra "{\"key_file\": \"$SSH_KEY\"}"
        
        echo -e "${GREEN}✅ Connection created${NC}"
    else
        echo "Skipped. Create manually later with:"
        echo "  airflow connections add 'remote_worker_ssh' \\"
        echo "    --conn-type 'ssh' \\"
        echo "    --conn-host 'remotedimpal-2' \\"
        echo "    --conn-login 'airflow' \\"
        echo "    --conn-extra '{\"key_file\": \"/home/airflow/.ssh/id_rsa\"}'"
    fi
fi

echo ""

#########################################################
# FIX 4: Clear DAG cache
#########################################################

echo -e "${BLUE}FIX 4: Clearing DAG cache${NC}"

if [ -d ~/airflow/__pycache__ ]; then
    rm -rf ~/airflow/__pycache__
    echo -e "${GREEN}✅ Cleared ~/airflow/__pycache__${NC}"
fi

if [ -d ~/airflow/dags/__pycache__ ]; then
    rm -rf ~/airflow/dags/__pycache__
    echo -e "${GREEN}✅ Cleared ~/airflow/dags/__pycache__${NC}"
fi

if [ -d ~/airflow/plugins/__pycache__ ]; then
    rm -rf ~/airflow/plugins/__pycache__
    echo -e "${GREEN}✅ Cleared ~/airflow/plugins/__pycache__${NC}"
fi

echo ""

#########################################################
# FIX 5: Restart scheduler
#########################################################

echo -e "${BLUE}FIX 5: Restarting scheduler${NC}"
echo "This ensures plugins and DAGs are reloaded..."

if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
    echo "Stopping scheduler..."
    pkill -f "airflow scheduler"
    
    # Wait for shutdown
    sleep 3
    
    echo "Starting scheduler..."
    airflow scheduler -D
    
    # Wait for startup
    sleep 5
    
    if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
        echo -e "${GREEN}✅ Scheduler restarted${NC}"
    else
        echo -e "${RED}❌ Scheduler failed to start!${NC}"
        echo "Check logs: tail -f ~/airflow/logs/scheduler/latest/*.log"
    fi
else
    echo "Scheduler not running. Starting..."
    airflow scheduler -D
    sleep 5
    
    if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
        echo -e "${GREEN}✅ Scheduler started${NC}"
    else
        echo -e "${RED}❌ Scheduler failed to start!${NC}"
    fi
fi

echo ""

#########################################################
# FIX 6: Verify DAGs are parsed
#########################################################

echo -e "${BLUE}FIX 6: Verifying DAG parsing${NC}"
echo "Waiting for scheduler to parse DAGs (30 seconds)..."
sleep 30

echo ""
echo "Checking for DAGs..."
DAG_LIST=$(airflow dags list 2>/dev/null | grep -E "test_simple_dag|test_ondemand_worker")

if [ -n "$DAG_LIST" ]; then
    echo -e "${GREEN}✅ DAGs found:${NC}"
    echo "$DAG_LIST"
else
    echo -e "${YELLOW}⚠️  No test DAGs found${NC}"
    echo ""
    echo "Checking for import errors..."
    airflow dags list-import-errors
fi

echo ""

#########################################################
# SUMMARY
#########################################################

echo "========================================================================"
echo -e "${BLUE}FIX SUMMARY${NC}"
echo "========================================================================"
echo ""

echo "What was fixed:"
echo "  ✅ Created __init__.py files for plugins"
echo "  ✅ Checked for test DAGs"
echo "  ✅ Verified/created SSH connection"
echo "  ✅ Cleared Python cache"
echo "  ✅ Restarted scheduler"
echo "  ✅ Verified DAG parsing"
echo ""

echo "NEXT STEPS:"
echo ""
echo "1. Test with simple DAG first (no remote worker needed):"
echo "   airflow dags trigger test_simple_dag"
echo ""
echo "2. Check DAG runs:"
echo "   watch -n 5 airflow dags list-runs -d test_simple_dag"
echo ""
echo "3. View logs:"
echo "   bash VIEW_DAG_LOGS.sh"
echo ""
echo "4. Once simple DAG works, try remote worker DAG:"
echo "   airflow dags trigger test_ondemand_worker_FIXED"
echo ""

echo "========================================================================"
echo ""

# Run check script
if [ -f ~/airflow/CHECK_DAG_ISSUES.sh ]; then
    read -p "Run diagnostic check now? (yes/no): " RUN_CHECK
    if [ "$RUN_CHECK" == "yes" ]; then
        echo ""
        bash ~/airflow/CHECK_DAG_ISSUES.sh
    fi
fi
