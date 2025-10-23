#!/bin/bash
#########################################################
# CHECK: Are Phase 1 Requirements Met?
# Run on Master Server
#########################################################

set +e  # Don't exit on errors

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================================================"
echo "  PHASE 1 REQUIREMENTS CHECK"
echo "========================================================================"
echo ""

MISSING=0

#########################################################
# PostgreSQL
#########################################################

echo -e "${BLUE}1. PostgreSQL 14+${NC}"

if command -v psql &> /dev/null; then
    PG_VERSION=$(psql --version | awk '{print $3}' | cut -d'.' -f1)
    echo "   Installed: psql version $(psql --version | awk '{print $3}')"
    
    if [ "$PG_VERSION" -ge 14 ]; then
        echo -e "   ${GREEN}✅ PostgreSQL 14+ found${NC}"
    else
        echo -e "   ${YELLOW}⚠️  PostgreSQL version < 14 (should upgrade)${NC}"
    fi
else
    echo -e "   ${RED}❌ PostgreSQL NOT installed${NC}"
    MISSING=$((MISSING + 1))
fi

# Check if PostgreSQL service is running
if systemctl is-active --quiet postgresql 2>/dev/null; then
    echo -e "   ${GREEN}✅ PostgreSQL service is running${NC}"
elif ps aux | grep -v grep | grep postgres > /dev/null; then
    echo -e "   ${GREEN}✅ PostgreSQL process is running${NC}"
else
    echo -e "   ${RED}❌ PostgreSQL service NOT running${NC}"
    MISSING=$((MISSING + 1))
fi

# Check if airflow database exists
if command -v psql &> /dev/null; then
    if sudo -u postgres psql -lqt 2>/dev/null | cut -d \| -f 1 | grep -qw airflow; then
        echo -e "   ${GREEN}✅ 'airflow' database exists${NC}"
    else
        echo -e "   ${RED}❌ 'airflow' database NOT found${NC}"
        MISSING=$((MISSING + 1))
    fi
fi

echo ""

#########################################################
# Redis
#########################################################

echo -e "${BLUE}2. Redis 6+${NC}"

if command -v redis-cli &> /dev/null; then
    REDIS_VERSION=$(redis-cli --version | awk '{print $2}' | cut -d'.' -f1)
    echo "   Installed: $(redis-cli --version)"
    
    if [ "$REDIS_VERSION" -ge 6 ]; then
        echo -e "   ${GREEN}✅ Redis 6+ found${NC}"
    else
        echo -e "   ${YELLOW}⚠️  Redis version < 6 (should upgrade)${NC}"
    fi
else
    echo -e "   ${RED}❌ Redis NOT installed${NC}"
    MISSING=$((MISSING + 1))
fi

# Check if Redis service is running
if systemctl is-active --quiet redis 2>/dev/null || systemctl is-active --quiet redis-server 2>/dev/null; then
    echo -e "   ${GREEN}✅ Redis service is running${NC}"
elif ps aux | grep -v grep | grep redis-server > /dev/null; then
    echo -e "   ${GREEN}✅ Redis process is running${NC}"
else
    echo -e "   ${RED}❌ Redis service NOT running${NC}"
    MISSING=$((MISSING + 1))
fi

# Test Redis connection
if command -v redis-cli &> /dev/null; then
    if redis-cli ping &>/dev/null | grep -q "PONG"; then
        echo -e "   ${GREEN}✅ Redis connection OK (no password)${NC}"
    else
        echo -e "   ${YELLOW}⚠️  Redis requires password (expected for production)${NC}"
    fi
fi

echo ""

#########################################################
# Python 3.12
#########################################################

echo -e "${BLUE}3. Python 3.12+${NC}"

if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | awk '{print $2}')
    echo "   Installed: Python $PYTHON_VERSION"
    
    if python3 -c 'import sys; exit(0 if sys.version_info >= (3, 12) else 1)'; then
        echo -e "   ${GREEN}✅ Python 3.12+ found${NC}"
    else
        echo -e "   ${YELLOW}⚠️  Python < 3.12 (may work but 3.12 recommended)${NC}"
    fi
else
    echo -e "   ${RED}❌ Python3 NOT found${NC}"
    MISSING=$((MISSING + 1))
fi

echo ""

#########################################################
# Airflow Installation
#########################################################

echo -e "${BLUE}4. Apache Airflow 2.10.2${NC}"

if command -v airflow &> /dev/null; then
    AIRFLOW_VERSION=$(airflow version 2>/dev/null)
    echo "   Installed: Airflow $AIRFLOW_VERSION"
    
    if [ "$AIRFLOW_VERSION" == "2.10.2" ]; then
        echo -e "   ${GREEN}✅ Airflow 2.10.2 (exact match)${NC}"
    else
        echo -e "   ${YELLOW}⚠️  Airflow version is $AIRFLOW_VERSION (2.10.2 recommended)${NC}"
    fi
else
    echo -e "   ${RED}❌ Airflow NOT installed${NC}"
    MISSING=$((MISSING + 1))
fi

echo ""

#########################################################
# Airflow Services
#########################################################

echo -e "${BLUE}5. Airflow Services${NC}"

# Scheduler
if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
    echo -e "   ${GREEN}✅ Scheduler is running${NC}"
else
    echo -e "   ${RED}❌ Scheduler NOT running${NC}"
    MISSING=$((MISSING + 1))
fi

# Webserver
if ps aux | grep -v grep | grep "airflow webserver" > /dev/null; then
    echo -e "   ${GREEN}✅ Webserver is running${NC}"
else
    echo -e "   ${YELLOW}⚠️  Webserver NOT running (not critical)${NC}"
fi

echo ""

#########################################################
# Configuration Check
#########################################################

echo -e "${BLUE}6. Airflow Configuration${NC}"

if [ -f ~/airflow/airflow.cfg ]; then
    echo -e "   ${GREEN}✅ airflow.cfg exists${NC}"
    
    # Check executor
    EXECUTOR=$(grep "^executor = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
    echo "   Executor: $EXECUTOR"
    
    # Check database
    SQL_CONN=$(grep "^sql_alchemy_conn = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
    if echo "$SQL_CONN" | grep -q "postgresql"; then
        echo -e "   ${GREEN}✅ PostgreSQL configured as database${NC}"
    elif echo "$SQL_CONN" | grep -q "sqlite"; then
        echo -e "   ${YELLOW}⚠️  SQLite configured (not suitable for CeleryExecutor)${NC}"
    fi
    
    # Check broker
    if [ "$EXECUTOR" == "CeleryExecutor" ]; then
        BROKER_URL=$(grep "^broker_url = " ~/airflow/airflow.cfg | cut -d'=' -f2 | xargs)
        if echo "$BROKER_URL" | grep -q "redis"; then
            echo -e "   ${GREEN}✅ Redis configured as broker${NC}"
        else
            echo -e "   ${RED}❌ Redis NOT configured as broker${NC}"
            MISSING=$((MISSING + 1))
        fi
    fi
else
    echo -e "   ${RED}❌ airflow.cfg NOT found${NC}"
    MISSING=$((MISSING + 1))
fi

echo ""

#########################################################
# Summary
#########################################################

echo "========================================================================"
echo -e "${BLUE}SUMMARY${NC}"
echo "========================================================================"
echo ""

if [ $MISSING -eq 0 ]; then
    echo -e "${GREEN}✅ ALL Phase 1 requirements are met!${NC}"
    echo ""
    echo "You can proceed to Phase 2 (Remote Worker Setup)"
    echo "See: PHASE2_COMPLETE_IMPLEMENTATION.html"
else
    echo -e "${RED}❌ $MISSING requirement(s) missing or not configured${NC}"
    echo ""
    echo -e "${YELLOW}RECOMMENDED ACTIONS:${NC}"
    echo ""
    
    if ! command -v psql &> /dev/null || ! systemctl is-active --quiet postgresql 2>/dev/null; then
        echo "1. Install PostgreSQL 14+"
        echo "   See section in: PHASE1_MASTER_SETUP_COMMANDS.HTML"
        echo ""
    fi
    
    if ! command -v redis-cli &> /dev/null || ! systemctl is-active --quiet redis-server 2>/dev/null; then
        echo "2. Install Redis 6+"
        echo "   See section in: PHASE1_MASTER_SETUP_COMMANDS.HTML"
        echo ""
    fi
    
    if [ ! -f ~/airflow/airflow.cfg ]; then
        echo "3. Initialize Airflow"
        echo "   Run: airflow db init"
        echo ""
    fi
    
    if echo "$SQL_CONN" | grep -q "sqlite" && [ "$EXECUTOR" == "CeleryExecutor" ]; then
        echo "4. Update airflow.cfg to use PostgreSQL"
        echo "   Change sql_alchemy_conn to: postgresql+psycopg2://airflow:PASSWORD@localhost:5432/airflow"
        echo ""
    fi
    
    echo "For complete Phase 1 setup instructions:"
    echo "   Open: PHASE1_MASTER_SETUP_COMMANDS.HTML in browser"
    echo ""
    echo "Or run automated script:"
    echo "   bash phase1_install.sh"
fi

echo ""
echo "========================================================================"
