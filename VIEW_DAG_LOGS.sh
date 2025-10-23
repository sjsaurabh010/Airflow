#!/bin/bash
#########################################################
# VIEW DAG LOGS - Easy log viewing for test_ondemand_worker
# Run on Master Server as airflow user
#########################################################

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

DAG_ID="test_ondemand_worker"

echo "========================================================================"
echo "  LOG VIEWER: $DAG_ID"
echo "========================================================================"
echo ""

#########################################################
# Function to show menu
#########################################################

show_menu() {
    echo ""
    echo -e "${BLUE}What would you like to view?${NC}"
    echo ""
    echo "  1) All task logs (latest run)"
    echo "  2) Specific task logs"
    echo "  3) Scheduler logs (for this DAG)"
    echo "  4) Live tail (follow latest task)"
    echo "  5) DAG run status"
    echo "  6) Search logs for errors"
    echo "  q) Quit"
    echo ""
}

#########################################################
# Function to find latest run_id
#########################################################

get_latest_run_id() {
    LATEST_RUN=$(airflow dags list-runs -d "$DAG_ID" 2>/dev/null | tail -n +4 | head -1 | awk '{print $3}')
    if [ -z "$LATEST_RUN" ]; then
        echo -e "${RED}❌ No DAG runs found!${NC}"
        echo "   Trigger DAG first: airflow dags trigger $DAG_ID"
        return 1
    fi
    echo "$LATEST_RUN"
}

#########################################################
# Function to list all logs for latest run
#########################################################

show_all_logs() {
    echo -e "${BLUE}Fetching logs for latest run...${NC}"
    
    RUN_ID=$(get_latest_run_id)
    if [ -z "$RUN_ID" ]; then
        return
    fi
    
    echo "Run ID: $RUN_ID"
    echo ""
    
    LOG_DIR=~/airflow/logs/dag_id=$DAG_ID
    if [ ! -d "$LOG_DIR" ]; then
        echo -e "${RED}❌ Log directory not found: $LOG_DIR${NC}"
        return
    fi
    
    # Find all log files for this run
    LOG_FILES=$(find "$LOG_DIR" -path "*$RUN_ID*.log" 2>/dev/null)
    
    if [ -z "$LOG_FILES" ]; then
        echo -e "${YELLOW}⚠️  No log files found for run: $RUN_ID${NC}"
        echo ""
        echo "Possible reasons:"
        echo "  - DAG is still queued (not started yet)"
        echo "  - Scheduler hasn't processed the run yet"
        echo "  - Check DAG status: airflow dags list-runs -d $DAG_ID"
        return
    fi
    
    echo "Found log files:"
    echo "$LOG_FILES" | while read -r logfile; do
        TASK_ID=$(echo "$logfile" | grep -oP 'task_id=\K[^/]+')
        echo ""
        echo "======================================================================"
        echo -e "${GREEN}TASK: $TASK_ID${NC}"
        echo "======================================================================"
        echo "File: $logfile"
        echo ""
        cat "$logfile"
    done
}

#########################################################
# Function to show specific task logs
#########################################################

show_task_logs() {
    echo ""
    echo "Available tasks in $DAG_ID:"
    echo "  - start_remote_worker"
    echo "  - worker_health_check"
    echo "  - test_worker_execution"
    echo "  - stop_remote_worker"
    echo ""
    read -p "Enter task ID: " TASK_ID
    
    if [ -z "$TASK_ID" ]; then
        echo "Cancelled."
        return
    fi
    
    RUN_ID=$(get_latest_run_id)
    if [ -z "$RUN_ID" ]; then
        return
    fi
    
    LOG_DIR=~/airflow/logs/dag_id=$DAG_ID
    LOG_FILE=$(find "$LOG_DIR" -path "*$RUN_ID*task_id=$TASK_ID*.log" 2>/dev/null | head -1)
    
    if [ -z "$LOG_FILE" ]; then
        echo -e "${RED}❌ Log file not found for task: $TASK_ID${NC}"
        
        # Show available logs
        echo ""
        echo "Available task logs:"
        find "$LOG_DIR" -name "*.log" 2>/dev/null | grep -oP 'task_id=\K[^/]+' | sort -u
        return
    fi
    
    echo ""
    echo "======================================================================"
    echo -e "${GREEN}TASK: $TASK_ID${NC}"
    echo "======================================================================"
    echo "File: $LOG_FILE"
    echo ""
    cat "$LOG_FILE"
}

#########################################################
# Function to show scheduler logs
#########################################################

show_scheduler_logs() {
    echo -e "${BLUE}Searching scheduler logs for $DAG_ID...${NC}"
    echo ""
    
    SCHED_DIR=~/airflow/logs/scheduler/latest
    if [ ! -d "$SCHED_DIR" ]; then
        echo -e "${RED}❌ Scheduler logs not found: $SCHED_DIR${NC}"
        return
    fi
    
    # Find scheduler logs mentioning this DAG
    RELEVANT_LOGS=$(grep -l "$DAG_ID" "$SCHED_DIR"/*.log 2>/dev/null)
    
    if [ -z "$RELEVANT_LOGS" ]; then
        echo -e "${YELLOW}⚠️  No scheduler logs mention $DAG_ID${NC}"
        return
    fi
    
    echo "$RELEVANT_LOGS" | while read -r logfile; do
        echo "======================================================================"
        echo "File: $logfile"
        echo "======================================================================"
        echo ""
        grep "$DAG_ID" "$logfile" | tail -20
        echo ""
    done
}

#########################################################
# Function to tail logs live
#########################################################

tail_logs() {
    echo -e "${BLUE}Finding latest log to tail...${NC}"
    
    LOG_DIR=~/airflow/logs/dag_id=$DAG_ID
    if [ ! -d "$LOG_DIR" ]; then
        echo -e "${RED}❌ Log directory not found${NC}"
        return
    fi
    
    # Find most recently modified log
    LATEST_LOG=$(find "$LOG_DIR" -name "*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
    
    if [ -z "$LATEST_LOG" ]; then
        echo -e "${RED}❌ No log files found${NC}"
        return
    fi
    
    echo "Tailing: $LATEST_LOG"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
    echo ""
    
    tail -f "$LATEST_LOG"
}

#########################################################
# Function to show DAG status
#########################################################

show_dag_status() {
    echo -e "${BLUE}DAG Run Status:${NC}"
    echo ""
    airflow dags list-runs -d "$DAG_ID" 2>/dev/null || echo "No runs found"
    echo ""
    
    echo -e "${BLUE}Task Instance Status:${NC}"
    echo ""
    RUN_ID=$(get_latest_run_id)
    if [ -n "$RUN_ID" ]; then
        airflow tasks list "$DAG_ID" 2>/dev/null || echo "No tasks found"
    fi
}

#########################################################
# Function to search for errors
#########################################################

search_errors() {
    echo -e "${BLUE}Searching for errors in logs...${NC}"
    echo ""
    
    LOG_DIR=~/airflow/logs/dag_id=$DAG_ID
    if [ ! -d "$LOG_DIR" ]; then
        echo -e "${RED}❌ Log directory not found${NC}"
        return
    fi
    
    ERRORS=$(find "$LOG_DIR" -name "*.log" -exec grep -l -i "error\|exception\|failed\|traceback" {} \; 2>/dev/null)
    
    if [ -z "$ERRORS" ]; then
        echo -e "${GREEN}✅ No errors found in logs${NC}"
        return
    fi
    
    echo -e "${RED}Found errors in these files:${NC}"
    echo "$ERRORS"
    echo ""
    
    read -p "View errors? (yes/no): " CONFIRM
    if [ "$CONFIRM" == "yes" ]; then
        echo "$ERRORS" | while read -r logfile; do
            echo ""
            echo "======================================================================"
            echo "File: $logfile"
            echo "======================================================================"
            grep -i -A 5 "error\|exception\|failed\|traceback" "$logfile"
        done
    fi
}

#########################################################
# Main loop
#########################################################

while true; do
    show_menu
    read -p "Choice: " CHOICE
    
    case $CHOICE in
        1) show_all_logs ;;
        2) show_task_logs ;;
        3) show_scheduler_logs ;;
        4) tail_logs ;;
        5) show_dag_status ;;
        6) search_errors ;;
        q|Q) echo "Goodbye!"; exit 0 ;;
        *) echo -e "${RED}Invalid choice${NC}" ;;
    esac
    
    echo ""
    read -p "Press ENTER to continue..."
done
