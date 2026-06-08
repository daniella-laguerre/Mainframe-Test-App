#!/bin/bash
set -e

echo "=============================================="
echo "  Financial Trading Platform - Starting Up"
echo "=============================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

cd "$(dirname "$0")/.."

# Phase 1: Infrastructure
echo -e "${YELLOW}Phase 1: Starting infrastructure (Kafka, Postgres, Redis, Elasticsearch)...${NC}"
docker compose up -d zookeeper postgres redis elasticsearch
echo "Waiting for Zookeeper..."
sleep 5
docker compose up -d kafka
echo "Waiting for Kafka..."
sleep 10

# Wait for Postgres
echo "Waiting for Postgres to be ready..."
until docker compose exec -T postgres pg_isready -U trading > /dev/null 2>&1; do
  sleep 2
done
echo -e "${GREEN}Postgres is ready.${NC}"

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
until docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1; do
  sleep 3
done
echo -e "${GREEN}Kafka is ready.${NC}"

# Phase 2: Start the replica
echo -e "${YELLOW}Phase 2: Starting Postgres replica...${NC}"
docker compose up -d postgres-replica

# Phase 3: Core services
echo -e "${YELLOW}Phase 3: Starting core services...${NC}"
docker compose up -d quote-service market-data-sim
sleep 5
docker compose up -d order-service risk-engine
sleep 5
docker compose up -d analytics event-processor
sleep 3
docker compose up -d gateway legacy-adapter batch-reconciler
sleep 3

# Phase 4: UI
echo -e "${YELLOW}Phase 4: Starting UI...${NC}"
docker compose up -d ui

# Phase 5: Traffic generator
echo -e "${YELLOW}Phase 5: Starting traffic generator...${NC}"
docker compose up -d traffic-generator

echo ""
echo -e "${GREEN}=============================================="
echo "  All services started!"
echo "==============================================${NC}"
echo ""
echo "  Trading UI:       http://localhost:8080"
echo "  API Gateway:      http://localhost:3000"
echo "  Order Service:    http://localhost:8001"
echo "  Quote Service:    http://localhost:8002"
echo "  Analytics:        http://localhost:8003"
echo "  Risk Engine:      http://localhost:8004"
echo "  Kafka:            localhost:9092"
echo "  Postgres:         localhost:5432"
echo "  Redis:            localhost:6379"
echo "  Elasticsearch:    http://localhost:9200"
echo ""
echo "  Useful commands:"
echo "    docker compose logs -f gateway          # API Gateway logs"
echo "    docker compose logs -f legacy-adapter   # COBOL/MQ legacy logs"
echo "    docker compose logs -f batch-reconciler # Batch job logs"
echo "    docker compose logs -f traffic-generator # Traffic stats"
echo "    docker compose logs -f event-processor  # Event processing"
echo "    docker compose logs -f                  # All logs (chaos mode)"
echo ""
