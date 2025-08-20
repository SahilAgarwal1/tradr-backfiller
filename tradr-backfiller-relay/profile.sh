#!/bin/bash

# Profiling script for tradr-backfiller-relay
# This script captures CPU and memory profiles to identify performance bottlenecks

set -e

PROFILE_DIR="profiles"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PPROF_URL="http://localhost:6060/debug/pprof"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Tradr Backfiller Profiling Tool ===${NC}"
echo ""

# Create profiles directory if it doesn't exist
mkdir -p $PROFILE_DIR

# Function to check if the service is running
check_service() {
    if ! curl -s "$PPROF_URL/" > /dev/null 2>&1; then
        echo -e "${RED}Error: pprof server is not accessible at $PPROF_URL${NC}"
        echo "Make sure the relay service is running with pprof enabled."
        exit 1
    fi
    echo -e "${GREEN}✓ pprof server is accessible${NC}"
}

# Function to capture CPU profile
capture_cpu_profile() {
    local duration=${1:-30}
    local filename="${PROFILE_DIR}/cpu_${TIMESTAMP}.prof"
    
    echo -e "${YELLOW}Capturing CPU profile for ${duration} seconds...${NC}"
    curl -s "${PPROF_URL}/profile?seconds=${duration}" -o "$filename"
    echo -e "${GREEN}✓ CPU profile saved to: $filename${NC}"
    
    # Generate text report
    echo -e "${YELLOW}Generating CPU profile report...${NC}"
    go tool pprof -text "$filename" > "${filename}.txt" 2>/dev/null || true
    echo -e "${GREEN}✓ Text report saved to: ${filename}.txt${NC}"
    
    # Show top functions
    echo ""
    echo -e "${GREEN}Top 10 CPU consuming functions:${NC}"
    go tool pprof -top -nodecount=10 "$filename" 2>/dev/null | head -20 || true
}

# Function to capture heap profile
capture_heap_profile() {
    local filename="${PROFILE_DIR}/heap_${TIMESTAMP}.prof"
    
    echo -e "${YELLOW}Capturing heap profile...${NC}"
    curl -s "${PPROF_URL}/heap" -o "$filename"
    echo -e "${GREEN}✓ Heap profile saved to: $filename${NC}"
    
    # Generate text report
    echo -e "${YELLOW}Generating heap profile report...${NC}"
    go tool pprof -text "$filename" > "${filename}.txt" 2>/dev/null || true
    echo -e "${GREEN}✓ Text report saved to: ${filename}.txt${NC}"
    
    # Show top memory allocations
    echo ""
    echo -e "${GREEN}Top 10 memory allocations:${NC}"
    go tool pprof -top -nodecount=10 "$filename" 2>/dev/null | head -20 || true
}

# Function to capture goroutine profile
capture_goroutine_profile() {
    local filename="${PROFILE_DIR}/goroutine_${TIMESTAMP}.prof"
    
    echo -e "${YELLOW}Capturing goroutine profile...${NC}"
    curl -s "${PPROF_URL}/goroutine" -o "$filename"
    echo -e "${GREEN}✓ Goroutine profile saved to: $filename${NC}"
    
    # Generate text report
    echo -e "${YELLOW}Generating goroutine report...${NC}"
    curl -s "${PPROF_URL}/goroutine?debug=1" > "${filename}.txt"
    echo -e "${GREEN}✓ Text report saved to: ${filename}.txt${NC}"
    
    # Show goroutine count
    echo ""
    echo -e "${GREEN}Goroutine summary:${NC}"
    curl -s "${PPROF_URL}/goroutine?debug=1" | grep -E "^[0-9]+ @" | wc -l | xargs echo "Total goroutines:"
}

# Function to capture block profile
capture_block_profile() {
    local filename="${PROFILE_DIR}/block_${TIMESTAMP}.prof"
    
    echo -e "${YELLOW}Capturing block profile (contention)...${NC}"
    curl -s "${PPROF_URL}/block" -o "$filename"
    echo -e "${GREEN}✓ Block profile saved to: $filename${NC}"
    
    # Generate text report
    go tool pprof -text "$filename" > "${filename}.txt" 2>/dev/null || true
    echo -e "${GREEN}✓ Text report saved to: ${filename}.txt${NC}"
}

# Function to capture mutex profile
capture_mutex_profile() {
    local filename="${PROFILE_DIR}/mutex_${TIMESTAMP}.prof"
    
    echo -e "${YELLOW}Capturing mutex profile...${NC}"
    curl -s "${PPROF_URL}/mutex" -o "$filename"
    echo -e "${GREEN}✓ Mutex profile saved to: $filename${NC}"
    
    # Generate text report
    go tool pprof -text "$filename" > "${filename}.txt" 2>/dev/null || true
    echo -e "${GREEN}✓ Text report saved to: ${filename}.txt${NC}"
}

# Function to capture all profiles
capture_all_profiles() {
    capture_cpu_profile 30
    echo ""
    capture_heap_profile
    echo ""
    capture_goroutine_profile
    echo ""
    capture_block_profile
    echo ""
    capture_mutex_profile
}

# Function to monitor live metrics
monitor_live() {
    echo -e "${GREEN}Monitoring live metrics (press Ctrl+C to stop)...${NC}"
    echo ""
    
    while true; do
        clear
        echo -e "${GREEN}=== Live Metrics at $(date) ===${NC}"
        echo ""
        
        # Get heap stats
        echo -e "${YELLOW}Memory Usage:${NC}"
        curl -s "${PPROF_URL}/heap?debug=1" 2>/dev/null | grep -E "^# Alloc|^# TotalAlloc|^# Sys|^# NumGC" || true
        echo ""
        
        # Get goroutine count
        echo -e "${YELLOW}Goroutines:${NC}"
        curl -s "${PPROF_URL}/goroutine?debug=1" 2>/dev/null | grep -E "^[0-9]+ @" | wc -l | xargs echo "Active:"
        echo ""
        
        # Get metrics from Prometheus endpoint if available
        echo -e "${YELLOW}Backfill Metrics:${NC}"
        curl -s "http://localhost:9090/metrics" 2>/dev/null | grep -E "relay_backfill|relay_operations_total|relay_bytes_processed" | tail -10 || echo "Metrics endpoint not available"
        
        sleep 5
    done
}

# Function to analyze profiles interactively
analyze_profile() {
    local profile=$1
    echo -e "${GREEN}Opening interactive pprof for: $profile${NC}"
    echo "Common commands:"
    echo "  top     - Show top functions"
    echo "  list <function> - Show source code"
    echo "  web     - Open visualization in browser"
    echo "  quit    - Exit"
    echo ""
    go tool pprof "$profile"
}

# Main menu
show_menu() {
    echo ""
    echo "What would you like to do?"
    echo "1) Capture CPU profile (30 seconds)"
    echo "2) Capture heap (memory) profile"
    echo "3) Capture goroutine profile"
    echo "4) Capture block profile (contention)"
    echo "5) Capture mutex profile"
    echo "6) Capture ALL profiles"
    echo "7) Monitor live metrics"
    echo "8) Analyze existing profile"
    echo "9) Exit"
    echo ""
    read -p "Select option [1-9]: " choice
}

# Main execution
check_service

if [ "$1" == "--all" ]; then
    capture_all_profiles
    exit 0
elif [ "$1" == "--cpu" ]; then
    capture_cpu_profile ${2:-30}
    exit 0
elif [ "$1" == "--heap" ]; then
    capture_heap_profile
    exit 0
elif [ "$1" == "--monitor" ]; then
    monitor_live
    exit 0
fi

# Interactive mode
while true; do
    show_menu
    
    case $choice in
        1)
            read -p "Duration in seconds [30]: " duration
            duration=${duration:-30}
            capture_cpu_profile $duration
            ;;
        2)
            capture_heap_profile
            ;;
        3)
            capture_goroutine_profile
            ;;
        4)
            capture_block_profile
            ;;
        5)
            capture_mutex_profile
            ;;
        6)
            capture_all_profiles
            ;;
        7)
            monitor_live
            ;;
        8)
            echo "Available profiles:"
            ls -la $PROFILE_DIR/*.prof 2>/dev/null || echo "No profiles found"
            read -p "Enter profile path: " profile_path
            if [ -f "$profile_path" ]; then
                analyze_profile "$profile_path"
            else
                echo -e "${RED}Profile not found: $profile_path${NC}"
            fi
            ;;
        9)
            echo -e "${GREEN}Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option${NC}"
            ;;
    esac
done