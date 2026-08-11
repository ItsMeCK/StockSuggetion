#!/bin/bash
# Midnight Sovereign - GCP e2-micro Deployment Script
# Run this script directly on your new GCP VM.

echo "======================================="
echo "🚀 INITIATING GCP ZERO-COST DEPLOYMENT 🚀"
echo "======================================="

# 1. System Updates & Dependencies
echo "[1/7] Updating system and installing dependencies..."
sudo apt-get update -y
sudo apt-get install -y python3-pip python3-venv git docker.io docker-compose curl

# 2. Add Swap Space (CRITICAL for e2-micro 1GB RAM)
# The engine runs 2 Postgres containers (Timescale + PGVector). 
# A 2GB Swap file prevents the VM from crashing due to Out-Of-Memory (OOM) errors.
echo "[2/7] Creating 2GB Swap File to protect against OOM..."
if [ ! -f /swapfile ]; then
    sudo fallocate -l 2G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "Swap file created successfully."
else
    echo "Swap file already exists."
fi

# 3. Clone Repository
echo "[3/7] Cloning Midnight Sovereign Repository..."
if [ -d "StockSuggetion" ]; then
    echo "Repo already exists. Pulling latest..."
    cd StockSuggetion
    git fetch origin
    git checkout live-engine-fixes
    git pull origin live-engine-fixes
else
    git clone -b live-engine-fixes https://github.com/ItsMeCK/StockSuggetion.git
    cd StockSuggetion
fi

# 4. Spin up Databases via Docker
echo "[4/7] Spinning up TimescaleDB and PGVector databases..."
sudo usermod -aG docker $USER
# We only want the databases, not the Python sync container right now
sudo docker-compose up -d timescaledb pgvector

# Wait for DBs to initialize
echo "Waiting 15 seconds for databases to boot..."
sleep 15

# 5. Initialize Schemas
echo "[5/7] Initializing Database Schemas..."
# timescaledb schema (market_data)
sudo docker exec -i timescaledb psql -U quant -d market_data < db/schema.sql
# pgvector schema (sovereign_state) - using Python init script
python3 init_ledger.py || echo "Warning: Ledger init failed or already exists."

# 6. Python Virtual Environment
echo "[6/7] Setting up Python Environment..."
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 7. Setup Crontab
echo "[7/7] Installing Trading Crontab..."
# Replace the paths in the crontab to match the GCP VM path (/home/$USER/StockSuggetion)
cat current_cron.txt | sed "s|/Users/poonamsalke/.gemini/antigravity/scratch/midnight_sovereign|/home/$USER/StockSuggetion|g" > gcp_cron.txt

crontab gcp_cron.txt
echo "Crontab installed successfully!"

echo "======================================="
echo "✅ DEPLOYMENT COMPLETE! ✅"
echo "======================================="
echo ""
echo "CRITICAL NEXT STEPS:"
echo "1. Create your .env file: nano .env"
echo "2. Paste your KITE_API_KEY and other credentials."
echo "3. Set LIVE_BUY=True to enable live trading."
echo "4. On your LAPTOP, set LIVE_BUY=False in your .env so they don't double-order."
