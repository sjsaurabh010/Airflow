 #!/bin/bash
     #########################################################
     # COMPLETE RESTART - Stop Everything and Restart
     #########################################################

     GREEN='\033[0;32m'
     RED='\033[0;31m'
     BLUE='\033[0;34m'
     NC='\033[0m'

     echo "========================================================================"
     echo "  COMPLETE AIRFLOW RESTART"
     echo "========================================================================"
     echo ""

     #########################################################
     # STEP 1: Pause All DAGs
     #########################################################

     echo -e "${BLUE}[1/6] Pausing all DAGs...${NC}"
     for dag in $(airflow dags list 2>/dev/null | tail -n +3 | awk '{print $1}'); do
         airflow dags pause "$dag" 2>/dev/null
         echo "  Paused: $dag"
     done
     echo -e "${GREEN}✓ All DAGs paused${NC}"
     echo ""

     #########################################################
     # STEP 2: Stop Celery Workers
     #########################################################

     echo -e "${BLUE}[2/6] Stopping Celery workers...${NC}"
     pkill -9 -f "celery worker" 2>/dev/null || true
     pkill -9 -f "celery.*airflow" 2>/dev/null || true
     sleep 2
     echo -e "${GREEN}✓ Celery workers stopped${NC}"
     echo ""

     #########################################################
     # STEP 3: Stop Scheduler
     #########################################################

     echo -e "${BLUE}[3/6] Stopping Airflow Scheduler...${NC}"
     pkill -f "airflow scheduler" 2>/dev/null || true
     sleep 3
     echo -e "${GREEN}✓ Scheduler stopped${NC}"
     echo ""

     #########################################################
     # STEP 4: Start Scheduler
     #########################################################

     echo -e "${BLUE}[4/6] Starting Airflow Scheduler...${NC}"
     airflow scheduler -D
     sleep 5

     if ps aux | grep -v grep | grep "airflow scheduler" > /dev/null; then
         echo -e "${GREEN}✓ Scheduler started${NC}"
     else
         echo -e "${RED}✗ Scheduler failed to start!${NC}"
         exit 1
     fi
     echo ""

     #########################################################
     # STEP 5: Start Celery Worker
     #########################################################

     echo -e "${BLUE}[5/6] Starting Celery worker...${NC}"
     nohup airflow celery worker > ~/airflow/logs/celery-worker.log 2>&1 &
     WORKER_PID=$!
     echo $WORKER_PID > ~/airflow/celery-worker.pid
     sleep 5

     if ps -p $WORKER_PID > /dev/null; then
         echo -e "${GREEN}✓ Celery worker started (PID: $WORKER_PID)${NC}"
     else
         echo -e "${RED}✗ Worker failed to start!${NC}"
         echo "Check logs: tail -f ~/airflow/logs/celery-worker.log"
         exit 1
     fi
     echo ""

     #########################################################
     # STEP 6: Unpause All DAGs
     #########################################################

     echo -e "${BLUE}[6/6] Unpausing all DAGs...${NC}"
     for dag in $(airflow dags list 2>/dev/null | tail -n +3 | awk '{print $1}'); do
         airflow dags unpause "$dag" 2>/dev/null
         echo "  Unpaused: $dag"
     done
     echo -e "${GREEN}✓ All DAGs unpaused${NC}"
     echo ""

     #########################################################
     # Verification
     #########################################################

     echo "========================================================================"
     echo -e "${GREEN}✓ RESTART COMPLETE!${NC}"
     echo "========================================================================"
     echo ""

     echo "Status:"
     echo "  Scheduler: $(ps aux | grep -v grep | grep 'airflow scheduler' > /dev/null && echo 'Running ✓' || echo 'Not running ✗')"
     echo "  Celery Worker: $(ps aux | grep -v grep | grep 'celery worker' > /dev/null && echo 'Running ✓' || echo 'Not running ✗')"
     echo ""

     echo "Active DAGs:"
     airflow dags list 2>/dev/null | head -10
     echo ""

     echo "Celery Worker Status:"
     celery -A airflow.providers.celery.executors.celery_executor.app inspect ping 2>/dev/null || echo "  Checking..."
     echo ""

     echo "========================================================================"
     echo "Usage:"
     echo "  Trigger DAG:  airflow dags trigger test_ondemand_worker"
     echo "  Monitor:      watch -n 2 'airflow dags list-runs -d test_ondemand_worker | head -5'"
     echo "  View logs:    tail -f ~/airflow/logs/celery-worker.log"
     echo "========================================================================"
