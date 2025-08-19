#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Relay Testing Suite${NC}"
echo "================================="

# Function to check if a service is healthy
check_service() {
    local service=$1
    local port=$2
    local max_attempts=30
    local attempt=0
    
    echo -n "Checking $service on port $port..."
    
    while [ $attempt -lt $max_attempts ]; do
        if nc -z localhost $port 2>/dev/null; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
        echo -n "."
    done
    
    echo -e " ${RED}✗${NC}"
    return 1
}

# Function to check logs for errors
check_logs() {
    local container=$1
    echo -e "\n${YELLOW}📋 Checking logs for $container:${NC}"
    
    # Get last 20 lines of logs
    docker logs --tail 20 $container 2>&1 | head -20
    
    # Check for specific error patterns
    if docker logs $container 2>&1 | grep -i "error\|fatal\|panic" | grep -v "Error receiving: EOF" | head -5; then
        echo -e "${YELLOW}⚠️  Found potential errors in $container logs${NC}"
    else
        echo -e "${GREEN}✓ No critical errors found${NC}"
    fi
}

# Function to test database connectivity
test_database() {
    echo -e "\n${YELLOW}🗄️  Testing Database:${NC}"
    
    # Check if tables were created
    docker exec relay-test-postgres psql -U relay -d backfiller_relay -c "\dt" 2>/dev/null | grep -E "last_seqs|backfill_jobs"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Database tables created successfully${NC}"
        
        # Check for any backfill jobs
        echo "Checking backfill jobs:"
        docker exec relay-test-postgres psql -U relay -d backfiller_relay -c "SELECT COUNT(*) as job_count FROM backfill_jobs;" 2>/dev/null
        
        # Check cursor position
        echo "Checking cursor position:"
        docker exec relay-test-postgres psql -U relay -d backfiller_relay -c "SELECT * FROM last_seqs;" 2>/dev/null
    else
        echo -e "${RED}✗ Database tables not found${NC}"
    fi
}

# Function to check metrics
check_metrics() {
    echo -e "\n${YELLOW}📊 Checking Metrics:${NC}"
    
    # Get metrics from relay
    curl -s http://localhost:9090/metrics | grep -E "relay_operations_total|relay_backfill_" | head -10
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Metrics endpoint is responding${NC}"
    else
        echo -e "${RED}✗ Metrics endpoint not responding${NC}"
    fi
}

# Function to monitor mock ingester
monitor_ingester() {
    echo -e "\n${YELLOW}📡 Monitoring Mock Ingester (10 seconds):${NC}"
    
    # Get initial stats
    docker logs relay-test-ingester 2>&1 | grep "JSON:" | tail -1 > /tmp/ingester_stats_before.txt
    
    echo "Waiting for operations to flow..."
    sleep 10
    
    # Get final stats
    docker logs relay-test-ingester 2>&1 | grep "JSON:" | tail -1 > /tmp/ingester_stats_after.txt
    
    # Show the difference
    if [ -s /tmp/ingester_stats_after.txt ]; then
        echo "Latest statistics:"
        cat /tmp/ingester_stats_after.txt | python3 -m json.tool 2>/dev/null || cat /tmp/ingester_stats_after.txt
    else
        echo -e "${YELLOW}No statistics yet - ingester may still be warming up${NC}"
    fi
    
    # Show recent operations if verbose mode
    echo -e "\nRecent operations:"
    docker logs --tail 20 relay-test-ingester 2>&1 | grep -E "CREATE|UPDATE|DELETE" | tail -5
}

# Main test flow
echo -e "\n${YELLOW}1. Starting services...${NC}"
docker-compose -f docker-compose.test.yml up -d

echo -e "\n${YELLOW}2. Waiting for services to be ready...${NC}"
sleep 5

echo -e "\n${YELLOW}3. Checking service health...${NC}"
check_service "PostgreSQL" 5433
check_service "Mock Ingester" 50052
check_service "Relay Metrics" 9090

echo -e "\n${YELLOW}4. Checking service logs...${NC}"
check_logs "relay-test"
check_logs "relay-test-ingester"

echo -e "\n${YELLOW}5. Testing components...${NC}"
test_database
check_metrics
monitor_ingester

echo -e "\n${YELLOW}6. Testing backfill trigger...${NC}"
# You can manually trigger a backfill by adding a repo to the backfill queue
echo "To manually trigger a backfill, run:"
echo "docker exec relay-test-postgres psql -U relay -d backfiller_relay -c \"INSERT INTO backfill_jobs (repo, state) VALUES ('did:plc:example', 'queued');\""

echo -e "\n${GREEN}✅ Testing complete!${NC}"
echo "================================="
echo -e "${YELLOW}Useful commands:${NC}"
echo "  • View relay logs:        docker logs -f relay-test"
echo "  • View ingester logs:     docker logs -f relay-test-ingester"
echo "  • View ingester stats:    docker logs relay-test-ingester | grep JSON | tail -1 | python3 -m json.tool"
echo "  • Check database:         docker exec relay-test-postgres psql -U relay -d backfiller_relay"
echo "  • Stop test environment:  docker-compose -f docker-compose.test.yml down"
echo ""
echo -e "${YELLOW}To run a longer test:${NC}"
echo "  Let it run for a few minutes and monitor the ingester stats to see operations flowing through."