#!/bin/bash
#########################################################
# PHASE 1: MASTER SERVER SETUP - AUTOMATED INSTALL
# Run as AIRFLOW user (after user creation)
# Based on PHASE1_MASTER_SETUP_COMMANDS.HTML
#########################################################

set -e  # Exit on error

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=================================================="
echo "  PHASE 1: MASTER SERVER INSTALLATION"
echo "  User: $(whoami)"
echo "  Host: $(hostname)"
echo "=================================================="
echo ""

# Check if running as airflow user
if [ "$(whoami)" != "airflow" ]; then
    echo -e "${RED}❌ ERROR: This script must run as 'airflow' user${NC}"
    echo "Run: su - airflow"
    exit 1
fi

#########################################################
# CONFIGURATION - EDIT THESE VALUES
#########################################################

AIRFLOW_VERSION="2.10.2"
PYTHON_VERSION="3.12"
POSTGRES_PASSWORD="YOUR_POSTGRES_PASSWORD_HERE"  # CHANGE THIS!
REDIS_PASSWORD="YOUR_REDIS_PASSWORD_HERE"        # CHANGE THIS!
AIRFLOW_ADMIN_PASSWORD="YOUR_ADMIN_PASSWORD_HERE" # CHANGE THIS!

echo -e "${YELLOW}⚠️  IMPORTANT: Edit this script and set passwords first!${NC}"
echo "Current passwords:"
echo "  POSTGRES_PASSWORD: $POSTGRES_PASSWORD"
echo "  REDIS_PASSWORD: $REDIS_PASSWORD"
echo "  AIRFLOW_ADMIN_PASSWORD: $AIRFLOW_ADMIN_PASSWORD"
echo ""
read -p "Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Exiting. Please edit the script and set passwords."
    exit 1
fi

#########################################################
# STEP 1: INSTALL POSTGRESQL 14
#########################################################

echo ""
echo -e "${BLUE}=== STEP 1: Installing PostgreSQL 14 ===${NC}"

# PostgreSQL requires root - display commands for root user
cat << 'ROOTCMD'

⚠️  PostgreSQL installation requires ROOT access.

Run these commands as ROOT (in another terminal):

----------------------------------------
# Install PostgreSQL 14
sudo apt-get install -y wget ca-certificates
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo apt-get update
sudo apt-get install -y postgresql-14 postgresql-client-14

# Configure PostgreSQL for Airflow
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'YOUR_POSTGRES_PASSWORD';"
sudo -u postgres psql -c "CREATE DATABASE airflow OWNER airflow;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;"

# Verify
sudo systemctl status postgresql
----------------------------------------

ROOTCMD

read -p "Press ENTER after PostgreSQL is installed by root..."

# Verify PostgreSQL connection
echo -e "${BLUE}Verifying PostgreSQL connection...${NC}"
export PGPASSWORD="$POSTGRES_PASSWORD"
if psql -h localhost -U airflow -d airflow -c "SELECT 1;" &>/dev/null; then
    echo -e "${GREEN}✅ PostgreSQL connection successful${NC}"
else
    echo -e "${RED}❌ PostgreSQL connection failed. Fix issues before continuing.${NC}"
    exit 1
fi

#########################################################
# STEP 2: INSTALL REDIS 6
#########################################################

echo ""
echo -e "${BLUE}=== STEP 2: Installing Redis 6+ ===${NC}"

cat << 'ROOTCMD'

⚠️  Redis installation requires ROOT access.

Run these commands as ROOT (in another terminal):

----------------------------------------
# Install Redis
sudo apt-get install -y redis-server

# Configure Redis password
sudo sed -i "s/# requirepass foobared/requirepass YOUR_REDIS_PASSWORD/" /etc/redis/redis.conf

# Restart Redis
sudo systemctl restart redis-server
sudo systemctl enable redis-server

# Verify
sudo systemctl status redis-server
----------------------------------------

ROOTCMD

read -p "Press ENTER after Redis is installed by root..."

# Verify Redis connection
echo -e "${BLUE}Verifying Redis connection...${NC}"
if redis-cli -a "$REDIS_PASSWORD" ping 2>/dev/null | grep -q "PONG"; then
    echo -e "${GREEN}✅ Redis connection successful${NC}"
else
    echo -e "${RED}❌ Redis connection failed. Fix issues before continuing.${NC}"
    exit 1
fi

#########################################################
# STEP 3: CREATE PYTHON VIRTUAL ENVIRONMENT
#########################################################

echo ""
echo -e "${BLUE}=== STEP 3: Creating Python Virtual Environment ===${NC}"

cd ~

# Create venv
python3 -m venv airflow-env
source airflow-env/bin/activate

# Upgrade pip
pip install --upgrade pip setuptools wheel

echo -e "${GREEN}✅ Virtual environment created and activated${NC}"

#########################################################
# STEP 4: INSTALL APACHE AIRFLOW 2.10.2
#########################################################

echo ""
echo -e "${BLUE}=== STEP 4: Installing Apache Airflow ${AIRFLOW_VERSION} ===${NC}"

# Set Airflow home
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Get constraints file
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Installing Airflow with CeleryExecutor..."
pip install "apache-airflow[celery,postgres,redis,ssh,slack]==${AIRFLOW_VERSION}" \
    --constraint "${CONSTRAINT_URL}"

# Install additional packages
pip install \
    celery[redis] \
    psycopg2-binary \
    redis \
    paramiko \
    sshtunnel

echo -e "${GREEN}✅ Airflow ${AIRFLOW_VERSION} installed${NC}"

# Verify
airflow version

#########################################################
# STEP 5: CONFIGURE AIRFLOW
#########################################################

echo ""
echo -e "${BLUE}=== STEP 5: Configuring Airflow ===${NC}"

# Initialize Airflow (creates airflow.cfg)
airflow db init

# Configure airflow.cfg
echo "Updating airflow.cfg..."

sed -i "s|^executor = .*|executor = CeleryExecutor|" ~/airflow/airflow.cfg
sed -i "s|^sql_alchemy_conn = .*|sql_alchemy_conn = postgresql+psycopg2://airflow:${POSTGRES_PASSWORD}@localhost:5432/airflow|" ~/airflow/airflow.cfg
sed -i "s|^broker_url = .*|broker_url = redis://:${REDIS_PASSWORD}@localhost:6379/0|" ~/airflow/airflow.cfg
sed -i "s|^result_backend = .*|result_backend = db+postgresql://airflow:${POSTGRES_PASSWORD}@localhost:5432/airflow|" ~/airflow/airflow.cfg
sed -i "s|^load_examples = .*|load_examples = False|" ~/airflow/airflow.cfg
sed -i "s|^dags_folder = .*|dags_folder = ${HOME}/dags|" ~/airflow/airflow.cfg

echo -e "${GREEN}✅ Configuration updated${NC}"

# Initialize database with new config
echo "Initializing database..."
airflow db migrate

#########################################################
# STEP 6: CREATE ADMIN USER
#########################################################

echo ""
echo -e "${BLUE}=== STEP 6: Creating Admin User ===${NC}"

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password "$AIRFLOW_ADMIN_PASSWORD"

echo -e "${GREEN}✅ Admin user created${NC}"

#########################################################
# STEP 7: CREATE SYSTEMD SERVICES
#########################################################

echo ""
echo -e "${BLUE}=== STEP 7: Creating Systemd Services ===${NC}"

cat << 'ROOTCMD'

⚠️  Systemd service setup requires ROOT access.

Run these commands as ROOT (in another terminal):

----------------------------------------
# Create webserver service
cat > /etc/systemd/system/airflow-webserver.service << 'EOF'
[Unit]
Description=Airflow webserver
After=network.target postgresql.service redis.service
Wants=postgresql.service redis.service

[Service]
Type=simple
User=airflow
Group=airflow
Environment="PATH=/home/airflow/airflow-env/bin"
Environment="AIRFLOW_HOME=/home/airflow/airflow"
ExecStart=/home/airflow/airflow-env/bin/airflow webserver
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
EOF

# Create scheduler service
cat > /etc/systemd/system/airflow-scheduler.service << 'EOF'
[Unit]
Description=Airflow scheduler
After=network.target postgresql.service redis.service
Wants=postgresql.service redis.service

[Service]
Type=simple
User=airflow
Group=airflow
Environment="PATH=/home/airflow/airflow-env/bin"
Environment="AIRFLOW_HOME=/home/airflow/airflow"
ExecStart=/home/airflow/airflow-env/bin/airflow scheduler
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
sudo systemctl daemon-reload

# Enable and start services
sudo systemctl enable airflow-webserver
sudo systemctl enable airflow-scheduler
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler

# Check status
sudo systemctl status airflow-webserver
sudo systemctl status airflow-scheduler
----------------------------------------

ROOTCMD

read -p "Press ENTER after services are started by root..."

#########################################################
# STEP 8: VERIFICATION
#########################################################

echo ""
echo -e "${BLUE}=== STEP 8: Verification ===${NC}"

echo "Checking services..."

# Give services time to start
sleep 10

# Check if webserver is running
if curl -s http://localhost:8080/health &>/dev/null; then
    echo -e "${GREEN}✅ Webserver is running on http://localhost:8080${NC}"
else
    echo -e "${YELLOW}⚠️  Webserver not responding yet (may need more time)${NC}"
fi

# Check database connection
if airflow db check; then
    echo -e "${GREEN}✅ Database connection OK${NC}"
else
    echo -e "${RED}❌ Database connection failed${NC}"
fi

# Check Redis connection
if redis-cli -a "$REDIS_PASSWORD" ping | grep -q "PONG"; then
    echo -e "${GREEN}✅ Redis connection OK${NC}"
else
    echo -e "${RED}❌ Redis connection failed${NC}"
fi

#########################################################
# COMPLETION
#########################################################

echo ""
echo "=================================================="
echo -e "${GREEN}✅ PHASE 1 INSTALLATION COMPLETE!${NC}"
echo "=================================================="
echo ""
echo "Access Airflow UI:"
echo "  URL: http://$(hostname -I | awk '{print $1}'):8080"
echo "  Username: admin"
echo "  Password: $AIRFLOW_ADMIN_PASSWORD"
echo ""
echo "Useful commands:"
echo "  Check webserver: sudo systemctl status airflow-webserver"
echo "  Check scheduler: sudo systemctl status airflow-scheduler"
echo "  View logs: tail -f ~/airflow/logs/scheduler/latest/*.log"
echo ""
echo "NEXT: Proceed to Phase 2 (Remote Worker Setup)"
echo "=================================================="
