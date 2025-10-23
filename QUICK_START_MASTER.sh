#!/bin/bash
#########################################################
# QUICK START: Create Airflow User & Begin Phase 1
# Run as ROOT on Master Server
#########################################################

set -e  # Exit on error

echo "=================================================="
echo "  MASTER SERVER SETUP - USER CREATION"
echo "=================================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Step 1: Creating airflow user...${NC}"

# Create airflow user
if id "airflow" &>/dev/null; then
    echo -e "${YELLOW}⚠️  User 'airflow' already exists. Skipping creation.${NC}"
else
    useradd -m -s /bin/bash airflow
    echo -e "${GREEN}✅ User 'airflow' created${NC}"
fi

# Set password (optional - comment out if using SSH keys only)
echo ""
echo -e "${YELLOW}Set password for airflow user (or press Ctrl+C to skip):${NC}"
passwd airflow || echo "Password setup skipped"

echo ""
echo -e "${BLUE}Step 2: Setting up directory structure...${NC}"

# Create basic directories
su - airflow -c "mkdir -p ~/airflow ~/logs ~/dags ~/plugins ~/.ssh"
su - airflow -c "chmod 700 ~/.ssh"

echo -e "${GREEN}✅ Directories created${NC}"

echo ""
echo -e "${BLUE}Step 3: Installing essential packages...${NC}"

# Update system
apt-get update

# Install essential packages
apt-get install -y \
    wget \
    curl \
    git \
    vim \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    python3-pip \
    python3-venv

echo -e "${GREEN}✅ Essential packages installed${NC}"

echo ""
echo -e "${BLUE}Step 4: Checking Python version...${NC}"

PYTHON_VERSION=$(python3 --version)
echo "Python version: $PYTHON_VERSION"

if python3 -c 'import sys; exit(0 if sys.version_info >= (3, 12) else 1)'; then
    echo -e "${GREEN}✅ Python 3.12+ detected${NC}"
else
    echo -e "${YELLOW}⚠️  Python 3.12 not found. Installing...${NC}"
    apt-get install -y software-properties-common
    add-apt-repository -y ppa:deadsnakes/ppa
    apt-get update
    apt-get install -y python3.12 python3.12-venv python3.12-dev
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1
    echo -e "${GREEN}✅ Python 3.12 installed${NC}"
fi

echo ""
echo -e "${BLUE}Step 5: Setting up SSH access for airflow user...${NC}"

# Generate SSH key for airflow user (if not exists)
if [ ! -f /home/airflow/.ssh/id_rsa ]; then
    su - airflow -c "ssh-keygen -t rsa -b 4096 -C 'airflow@master' -f ~/.ssh/id_rsa -N ''"
    echo -e "${GREEN}✅ SSH key generated${NC}"
else
    echo -e "${YELLOW}⚠️  SSH key already exists${NC}"
fi

# Display public key
echo ""
echo -e "${YELLOW}Public SSH key for airflow user (save this for remote workers):${NC}"
cat /home/airflow/.ssh/id_rsa.pub

echo ""
echo -e "${BLUE}Step 6: Adding airflow user to sudoers (optional for troubleshooting)...${NC}"
if ! grep -q "^airflow" /etc/sudoers; then
    echo "airflow ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart airflow-*" >> /etc/sudoers.d/airflow
    chmod 440 /etc/sudoers.d/airflow
    echo -e "${GREEN}✅ Sudo access configured (limited to Airflow services)${NC}"
else
    echo -e "${YELLOW}⚠️  Sudo access already configured${NC}"
fi

echo ""
echo "=================================================="
echo -e "${GREEN}✅ USER SETUP COMPLETE!${NC}"
echo "=================================================="
echo ""
echo "NEXT STEPS:"
echo ""
echo "1. Switch to airflow user:"
echo -e "   ${BLUE}su - airflow${NC}"
echo ""
echo "2. Run Phase 1 setup script:"
echo -e "   ${BLUE}bash phase1_install.sh${NC}"
echo ""
echo "OR manually follow commands in:"
echo "   PHASE1_MASTER_SETUP_COMMANDS.HTML"
echo ""
echo "=================================================="
