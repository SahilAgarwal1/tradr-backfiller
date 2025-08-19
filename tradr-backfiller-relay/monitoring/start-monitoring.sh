#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting Relay Monitoring Stack${NC}"
echo "================================="

# Stop any existing containers
echo -e "${YELLOW}Stopping existing containers...${NC}"
docker-compose -f docker-compose.monitoring.yml down

# Start the monitoring stack
echo -e "${YELLOW}Starting services...${NC}"
docker-compose -f docker-compose.monitoring.yml up -d

# Wait for services to be ready
echo -e "${YELLOW}Waiting for services to initialize...${NC}"
sleep 10

# Check service health
check_service() {
    local name=$1
    local port=$2
    if nc -z localhost $port 2>/dev/null; then
        echo -e "${GREEN}✓ $name is running on port $port${NC}"
    else
        echo -e "${RED}✗ $name is not accessible on port $port${NC}"
    fi
}

echo -e "\n${YELLOW}Checking services:${NC}"
check_service "PostgreSQL" 5434
check_service "Mock Ingester" 50053
check_service "Relay Metrics" 9090
check_service "Prometheus" 9091
check_service "Grafana" 3000
check_service "Postgres Exporter" 9187

echo -e "\n${GREEN}📊 Monitoring Stack Ready!${NC}"
echo "================================="
echo -e "${BLUE}Access points:${NC}"
echo "  • Grafana Dashboard: ${GREEN}http://localhost:3000${NC}"
echo "    Username: admin / Password: admin"
echo "  • Prometheus: ${GREEN}http://localhost:9091${NC}"
echo "  • Relay Metrics: ${GREEN}http://localhost:9090/metrics${NC}"
echo ""
echo -e "${YELLOW}Dashboard features:${NC}"
echo "  ✓ Real-time operations throughput"
echo "  ✓ Backfill queue monitoring"
echo "  ✓ Active backfills tracking"
echo "  ✓ Processing rates and latencies"
echo "  ✓ Error rates and buffer status"
echo "  ✓ Firehose cursor position"
echo ""
echo -e "${YELLOW}Useful commands:${NC}"
echo "  • View logs: docker-compose -f docker-compose.monitoring.yml logs -f relay"
echo "  • Check DB: docker exec relay-monitor-postgres psql -U relay -d backfiller_relay -c 'SELECT state, COUNT(*) FROM gorm_db_jobs GROUP BY state;'"
echo "  • Stop all: docker-compose -f docker-compose.monitoring.yml down"
echo ""
echo -e "${GREEN}Open http://localhost:3000 to view the dashboard!${NC}"