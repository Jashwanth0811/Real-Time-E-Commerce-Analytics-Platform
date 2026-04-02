#!/usr/bin/env bash
# ============================================================
# E-Commerce Pipeline — Quick Start Script
# Usage: ./scripts/setup.sh
# ============================================================

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log()    { echo -e "${GREEN}[✓]${NC} $1"; }
info()   { echo -e "${BLUE}[→]${NC} $1"; }
warn()   { echo -e "${YELLOW}[!]${NC} $1"; }
error()  { echo -e "${RED}[✗]${NC} $1"; exit 1; }

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   E-Commerce Analytics Pipeline — Setup          ║${NC}"
echo -e "${BLUE}║   Kafka · PySpark · dbt · Airflow · Streamlit    ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════╝${NC}"
echo ""

# ─── Pre-flight checks ───────────────────────────────────────

info "Checking prerequisites..."

command -v docker      >/dev/null 2>&1 || error "Docker not found. Install: https://docs.docker.com/get-docker/"
command -v docker-compose >/dev/null 2>&1 || \
  docker compose version >/dev/null 2>&1  || \
  error "docker-compose not found."
command -v python3     >/dev/null 2>&1 || error "Python 3 not found."

DOCKER_VER=$(docker --version | grep -oE '[0-9]+\.[0-9]+' | head -1)
log "Docker $DOCKER_VER detected"

# Check Docker is running
docker info >/dev/null 2>&1 || error "Docker daemon not running. Please start Docker Desktop."

# ─── Python virtualenv ───────────────────────────────────────

info "Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    log "Virtual environment created"
fi

source venv/bin/activate

info "Installing Python dependencies..."
pip install --quiet --upgrade pip
pip install --quiet \
    kafka-python==2.0.2 \
    faker==22.6.0 \
    psycopg2-binary==2.9.9 \
    pandas==2.2.1 \
    dbt-postgres==1.7.9 \
    python-dotenv==1.0.1

log "Python dependencies installed"

# ─── Create data directories ─────────────────────────────────

info "Creating local directories..."
mkdir -p data/bronze data/silver data/gold logs
log "Directories created"

# ─── Start Docker services ────────────────────────────────────

info "Starting infrastructure services (Kafka, Postgres, Airflow, Dashboard)..."
info "This may take 2-3 minutes on first run (pulling images)..."

if docker compose version >/dev/null 2>&1; then
    DC="docker compose"
else
    DC="docker-compose"
fi

$DC up -d --build

log "Docker services started"

# ─── Wait for services ────────────────────────────────────────

info "Waiting for PostgreSQL to be ready..."
MAX_TRIES=30
for i in $(seq 1 $MAX_TRIES); do
    if $DC exec -T postgres pg_isready -U warehouse -d ecommerce_dw >/dev/null 2>&1; then
        log "PostgreSQL ready"
        break
    fi
    if [ $i -eq $MAX_TRIES ]; then
        error "PostgreSQL did not become ready in time"
    fi
    echo -n "."
    sleep 3
done

info "Waiting for Kafka to be ready..."
for i in $(seq 1 30); do
    if $DC exec -T kafka kafka-broker-api-versions \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        log "Kafka ready"
        break
    fi
    echo -n "."
    sleep 4
done
echo ""

# ─── Create Kafka topics ──────────────────────────────────────

info "Creating Kafka topics..."
$DC exec -T kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic ecommerce.events \
    --partitions 3 \
    --replication-factor 1 2>/dev/null || true

$DC exec -T kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic ecommerce.orders \
    --partitions 2 \
    --replication-factor 1 2>/dev/null || true

$DC exec -T kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic ecommerce.sessions \
    --partitions 2 \
    --replication-factor 1 2>/dev/null || true

log "Kafka topics created: ecommerce.events, ecommerce.orders, ecommerce.sessions"

# ─── Seed initial data ────────────────────────────────────────

info "Seeding 30 days of historical data into Bronze..."
python3 scripts/seed_historical.py
log "Historical data seeded"

# ─── Run initial pipeline ─────────────────────────────────────

info "Running initial Bronze→Silver→Gold pipeline..."
python3 scripts/run_pipeline.py
log "Initial pipeline complete"

# ─── Setup Airflow connection ─────────────────────────────────

info "Configuring Airflow connection..."
sleep 5
$DC exec -T airflow airflow connections add postgres_warehouse \
    --conn-type postgres \
    --conn-host postgres \
    --conn-login warehouse \
    --conn-password warehouse123 \
    --conn-schema ecommerce_dw \
    --conn-port 5432 2>/dev/null || \
$DC exec -T airflow airflow connections delete postgres_warehouse 2>/dev/null && \
$DC exec -T airflow airflow connections add postgres_warehouse \
    --conn-type postgres \
    --conn-host postgres \
    --conn-login warehouse \
    --conn-password warehouse123 \
    --conn-schema ecommerce_dw \
    --conn-port 5432 2>/dev/null || warn "Airflow connection setup — check manually if needed"

log "Airflow connection configured"

# ─── Summary ─────────────────────────────────────────────────

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║            🎉 Setup Complete!                     ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BLUE}📊 Dashboard:${NC}    http://localhost:8501"
echo -e "  ${BLUE}✈️  Airflow:${NC}     http://localhost:8081  (admin / admin123)"
echo -e "  ${BLUE}📨 Kafka UI:${NC}    http://localhost:8080"
echo -e "  ${BLUE}🐘 PostgreSQL:${NC}  localhost:5432  (warehouse / warehouse123)"
echo ""
echo -e "  ${YELLOW}Next steps:${NC}"
echo -e "  1. Open http://localhost:8501 to see live dashboard"
echo -e "  2. Run producer:  ${GREEN}source venv/bin/activate && python3 kafka_producer/producer.py --rate 10${NC}"
echo -e "  3. Trigger DAG:   Airflow UI → daily_ecommerce_pipeline → Trigger"
echo -e "  4. Stop all:      ${GREEN}docker compose down${NC}"
echo ""
