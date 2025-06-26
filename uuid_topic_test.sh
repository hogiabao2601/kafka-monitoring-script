#!/bin/bash

# UUID Topic Creation Test Script
# Creates test topics with UUID names to simulate client requests

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SSL_CONFIG_PATH="${SSL_CONFIG_PATH:-../config/ssl.conf}"
PRODUCER_CONFIG_PATH="${PRODUCER_CONFIG_PATH:-../config/producer.properties}"
CONSUMER_CONFIG_PATH="${CONSUMER_CONFIG_PATH:-../config/consumer.properties}"
TOPIC_PREFIX="${TOPIC_PREFIX:-}"
DEFAULT_PARTITIONS="${DEFAULT_PARTITIONS:-3}"
DEFAULT_REPLICATION="${DEFAULT_REPLICATION:-1}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Function to generate UUID
generate_uuid() {
    # Generate UUID v4 format
    local uuid=$(cat /proc/sys/kernel/random/uuid 2>/dev/null)
    if [ -z "$uuid" ]; then
        # Fallback UUID generation
        local hex_chars="0123456789abcdef"
        local uuid=""
        for i in {1..32}; do
            local rand_char=${hex_chars:$((RANDOM % 16)):1}
            uuid+="$rand_char"
            if [[ $i -eq 8 || $i -eq 12 || $i -eq 16 || $i -eq 20 ]]; then
                uuid+="-"
            fi
        done
    fi
    echo "$uuid"
}

# Function to create a single UUID topic with timing
create_uuid_topic() {
    echo "--------"
    local partitions="${1:-$DEFAULT_PARTITIONS}"
    local replication="${2:-$DEFAULT_REPLICATION}"
    local prefix="${3:-$TOPIC_PREFIX}"
    echo $prefix
    local uuid=$(generate_uuid)
    local topic_name="${prefix}${uuid}"
    echo $prefix
    
    echo -e "${BLUE}🚀 Creating topic: $topic_name${NC}"
    echo "  UUID: $uuid"
    echo "  Partitions: $partitions"
    echo "  Replication Factor: $replication"
    
    # Measure creation time
    local start_time=$(date +%s%3N)
    local start_readable=$(date '+%Y-%m-%d %H:%M:%S.%3N')
    
    # Create the topic
    local create_output
    create_output=$(./kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH"\
                   --create \
                   --topic "$topic_name" \
                   --partitions "$partitions" \
                   --replication-factor "$replication" 2>&1)
    
    local create_result=$?
    local end_time=$(date +%s%3N)
    local end_readable=$(date '+%Y-%m-%d %H:%M:%S.%3N')
    local creation_latency=$((end_time - start_time))
    
    if [ $create_result -eq 0 ]; then
        echo -e "  ${GREEN}✅ Topic created successfully${NC}"
        echo "  Creation Time: $start_readable"
        echo "  Completion Time: $end_readable"
        echo "  Creation Latency: ${creation_latency}ms"
        
        # Verify topic is ready
        echo "  🔍 Verifying topic readiness..."
        local verify_start=$(date +%s%3N)
        
        local ready=false
        local max_attempts=60  # 6 seconds max
        local attempt=0
        
        while [ $attempt -lt $max_attempts ] && [ "$ready" = false ]; do
            if ./kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH"\
               --describe --topic "$topic_name" &>/dev/null; then
                ready=true
            else
                sleep 0.1
                ((attempt++))
            fi
        done
        
        local verify_end=$(date +%s%3N)
        local verify_latency=$((verify_end - verify_start))
        
        if [ "$ready" = true ]; then
            echo -e "  ${GREEN}✅ Topic ready for operations (${verify_latency}ms)${NC}"
            
            # Get topic details
            local topic_details
            topic_details=$(./kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH"\
                           --describe --topic "$topic_name" 2>/dev/null)
            
            echo "  📊 Topic Details:"
            echo "$topic_details" | grep -E "(Topic:|PartitionCount:|ReplicationFactor:)" | sed 's/^/    /'
            
        else
            echo -e "  ${YELLOW}⚠️  Topic not ready within timeout (${verify_latency}ms)${NC}"
        fi
        
        # Log the results
        local log_entry="$(date '+%Y-%m-%d %H:%M:%S'),$topic_name,$creation_latency,$verify_latency,$partitions,$replication,success"
        echo "$log_entry" >> "topic_creation_test.csv"
        
        echo "  📝 Logged: $topic_name"
        echo ""
        return 0
        
    else
        echo -e "  ${RED}❌ Topic creation failed${NC}"
        echo "  Error: $create_output"
        echo "  Creation Latency: ${creation_latency}ms"
        
        # Log the failure
        local log_entry="$(date '+%Y-%m-%d %H:%M:%S'),$topic_name,$creation_latency,0,$partitions,$replication,failed"
        echo "$log_entry" >> "topic_creation_test.csv"
        
        echo ""
        return 1
    fi
}

# Function to create multiple topics in sequence
create_multiple_topics() {
    local count="$1"
    local partitions="${2:-$DEFAULT_PARTITIONS}"
    local replication="${3:-$DEFAULT_REPLICATION}"
    local interval="${4:-1}" # seconds between creations
    
    echo -e "${BLUE}🚀 Creating $count UUID topics...${NC}"
    echo "Configuration:"
    echo "  Count: $count"
    echo "  Partitions per topic: $partitions"
    echo "  Replication Factor: $replication"
    echo "  Interval: ${interval}s"
    echo ""
    
    # Initialize CSV log
    if [ ! -f "topic_creation_test.csv" ]; then
        echo "timestamp,topic_name,creation_latency_ms,verify_latency_ms,partitions,replication_factor,status" > "topic_creation_test.csv"
    fi
    
    local successful=0
    local failed=0
    local total_creation_time=0
    local start_batch=$(date +%s%3N)
    
    for ((i=1; i<=count; i++)); do
        echo -e "${YELLOW}Creating topic $i of $count${NC}"
        
        if create_uuid_topic "$partitions" "$replication"; then
            ((successful++))
        else
            ((failed++))
        fi
        
        # Wait between creations (except for last one)
        if [ $i -lt $count ] && [ "$interval" != "0" ]; then
            sleep "$interval"
        fi
    done
    
    local end_batch=$(date +%s%3N)
    local total_batch_time=$((end_batch - start_batch))
    
    echo "=== Batch Creation Summary ==="
    echo "Total Topics: $count"
    echo "Successful: $successful"
    echo "Failed: $failed"
    echo "Success Rate: $(echo "scale=1; $successful * 100 / $count" | bc -l)%"
    echo "Total Batch Time: ${total_batch_time}ms"
    echo "Average Time per Topic: $(echo "scale=1; $total_batch_time / $count" | bc -l)ms"
    echo "Throughput: $(echo "scale=2; $count * 1000 / $total_batch_time" | bc -l) topics/second"
    echo ""
}

# Function to create topics in parallel
create_parallel_topics() {
    local count="$1"
    local partitions="${2:-$DEFAULT_PARTITIONS}"
    local replication="${3:-$DEFAULT_REPLICATION}"
    local max_parallel="${4:-5}" # max parallel processes
    
    echo -e "${BLUE}🚀 Creating $count UUID topics in parallel (max $max_parallel concurrent)...${NC}"
    echo "Configuration:"
    echo "  Count: $count"
    echo "  Partitions per topic: $partitions"
    echo "  Replication Factor: $replication"
    echo "  Max Parallel: $max_parallel"
    echo ""
    
    # Initialize CSV log
    if [ ! -f "topic_creation_test.csv" ]; then
        echo "timestamp,topic_name,creation_latency_ms,verify_latency_ms,partitions,replication_factor,status" > "topic_creation_test.csv"
    fi
    
    local start_batch=$(date +%s%3N)
    local pids=()
    local completed=0
    
    # Function to wait for available slot
    wait_for_slot() {
        while [ ${#pids[@]} -ge $max_parallel ]; do
            local new_pids=()
            for pid in "${pids[@]}"; do
                if kill -0 "$pid" 2>/dev/null; then
                    new_pids+=("$pid")
                else
                    ((completed++))
                    echo "  Completed: $completed/$count"
                fi
            done
            pids=("${new_pids[@]}")
            sleep 0.1
        done
    }
    
    # Start parallel topic creation
    for ((i=1; i<=count; i++)); do
        wait_for_slot
        
        # Create topic in background
        (create_uuid_topic "$partitions" "$replication" > "/tmp/topic_creation_$i.log" 2>&1) &
        pids+=($!)
        
        echo "  Started: $i/$count (Active: ${#pids[@]})"
    done
    
    # Wait for all to complete
    echo "  Waiting for all topics to complete..."
    for pid in "${pids[@]}"; do
        wait "$pid"
        ((completed++))
    done
    
    local end_batch=$(date +%s%3N)
    local total_batch_time=$((end_batch - start_batch))
    
    # Collect results from log files
    local successful=0
    local failed=0
    
    for ((i=1; i<=count; i++)); do
        if grep -q "Topic created successfully" "/tmp/topic_creation_$i.log" 2>/dev/null; then
            ((successful++))
        else
            ((failed++))
        fi
        # Clean up log file
        rm -f "/tmp/topic_creation_$i.log"
    done
    
    echo ""
    echo "=== Parallel Creation Summary ==="
    echo "Total Topics: $count"
    echo "Successful: $successful"
    echo "Failed: $failed"
    echo "Success Rate: $(echo "scale=1; $successful * 100 / $count" | bc -l)%"
    echo "Total Batch Time: ${total_batch_time}ms"
    echo "Average Time per Topic: $(echo "scale=1; $total_batch_time / $count" | bc -l)ms"
    echo "Effective Throughput: $(echo "scale=2; $count * 1000 / $total_batch_time" | bc -l) topics/second"
    echo ""
}

# Function to stress test topic creation
stress_test() {
    local duration_seconds="$1"
    local rate_per_second="${2:-1}"
    local partitions="${3:-$DEFAULT_PARTITIONS}"
    local replication="${4:-$DEFAULT_REPLICATION}"
    
    echo -e "${BLUE}🔥 Starting stress test...${NC}"
    echo "Configuration:"
    echo "  Duration: ${duration_seconds}s"
    echo "  Rate: $rate_per_second topics/second"
    echo "  Partitions per topic: $partitions"
    echo "  Replication Factor: $replication"
    echo ""
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local interval_ms=$(echo "scale=3; 1000 / $rate_per_second" | bc -l)
    local count=0
    local successful=0
    local failed=0
    
    echo "Press Ctrl+C to stop early"
    echo ""
    
    while [ $(date +%s) -lt $end_time ]; do
        local creation_start=$(date +%s%3N)
        
        if create_uuid_topic "$partitions" "$replication" > "/dev/null" 2>&1; then
            ((successful++))
        else
            ((failed++))
        fi
        ((count++))
        
        local creation_end=$(date +%s%3N)
        local creation_time=$((creation_end - creation_start))
        
        # Calculate how long to sleep
        local elapsed_ms=$((creation_end - creation_start))
        local sleep_time=$(echo "scale=3; ($interval_ms - $elapsed_ms) / 1000" | bc -l)
        
        if [ "$(echo "$sleep_time > 0" | bc -l)" -eq 1 ]; then
            sleep "$sleep_time"
        fi
        
        # Progress update every 10 topics
        if [ $((count % 10)) -eq 0 ]; then
            local elapsed=$(($(date +%s) - start_time))
            local current_rate=$(echo "scale=2; $count / $elapsed" | bc -l)
            echo "  Progress: $count topics in ${elapsed}s (rate: $current_rate/s, success: $successful, failed: $failed)"
        fi
    done
    
    local actual_duration=$(($(date +%s) - start_time))
    local actual_rate=$(echo "scale=2; $count / $actual_duration" | bc -l)
    
    echo ""
    echo "=== Stress Test Results ==="
    echo "Duration: ${actual_duration}s"
    echo "Total Topics: $count"
    echo "Successful: $successful"
    echo "Failed: $failed"
    echo "Success Rate: $(echo "scale=1; $successful * 100 / $count" | bc -l)%"
    echo "Target Rate: $rate_per_second topics/s"
    echo "Actual Rate: $actual_rate topics/s"
    echo ""
}

# Function to benchmark topic creation performance
benchmark() {
    local test_counts=(1 5 10 20 50)
    local partitions="${1:-$DEFAULT_PARTITIONS}"
    local replication="${2:-$DEFAULT_REPLICATION}"
    
    echo -e "${BLUE}📊 Running topic creation benchmark...${NC}"
    echo "Configuration:"
    echo "  Partitions per topic: $partitions"
    echo "  Replication Factor: $replication"
    echo ""
    
    # Initialize benchmark results file
    local benchmark_file="topic_creation_benchmark_$(date +%Y%m%d_%H%M%S).csv"
    echo "test_count,total_time_ms,avg_time_ms,throughput_per_sec,success_rate" > "$benchmark_file"
    
    for count in "${test_counts[@]}"; do
        echo -e "${YELLOW}Testing with $count topics...${NC}"
        
        local start_time=$(date +%s%3N)
        local successful=0
        local failed=0
        
        for ((i=1; i<=count; i++)); do
            if create_uuid_topic "$partitions" "$replication" > "/dev/null" 2>&1; then
                ((successful++))
            else
                ((failed++))
            fi
        done
        
        local end_time=$(date +%s%3N)
        local total_time=$((end_time - start_time))
        local avg_time=$(echo "scale=2; $total_time / $count" | bc -l)
        local throughput=$(echo "scale=2; $count * 1000 / $total_time" | bc -l)
        local success_rate=$(echo "scale=1; $successful * 100 / $count" | bc -l)
        
        echo "  Results: ${total_time}ms total, ${avg_time}ms avg, ${throughput} topics/s, ${success_rate}% success"
        
        # Log to benchmark file
        echo "$count,$total_time,$avg_time,$throughput,$success_rate" >> "$benchmark_file"
        
        # Brief pause between tests
        sleep 2
    done
    
    echo ""
    echo -e "${GREEN}✅ Benchmark complete. Results saved to: $benchmark_file${NC}"
    echo ""
}

# Function to show usage
show_usage() {
    cat << EOF
UUID Topic Creation Test Script

USAGE:
  $0 [COMMAND] [OPTIONS]

COMMANDS:
  single [PARTITIONS] [REPLICATION]     Create a single UUID topic
  multiple COUNT [PARTITIONS] [REPLICATION] [INTERVAL]  Create multiple topics sequentially
  parallel COUNT [PARTITIONS] [REPLICATION] [MAX_PARALLEL]  Create multiple topics in parallel
  stress DURATION RATE [PARTITIONS] [REPLICATION]  Stress test topic creation
  benchmark [PARTITIONS] [REPLICATION]  Run performance benchmark
  help                                  Show this help message

OPTIONS:
  --bootstrap-servers SERVER           Kafka bootstrap servers (default: localhost:9092)
  --prefix PREFIX                      Topic prefix (default: user-request-)

ENVIRONMENT VARIABLES:
  KAFKA_BOOTSTRAP_SERVERS              Bootstrap servers
  TOPIC_PREFIX                         Topic prefix
  DEFAULT_PARTITIONS                   Default partition count (default: 3)
  DEFAULT_REPLICATION                  Default replication factor (default: 1)

EXAMPLES:
  $0 single                            # Create one topic with defaults
  $0 single 5 2                       # Create one topic with 5 partitions, RF=2
  $0 multiple 10                       # Create 10 topics sequentially
  $0 multiple 10 3 1 0.5              # Create 10 topics with 0.5s interval
  $0 parallel 20 3 1 5                # Create 20 topics, max 5 parallel
  $0 stress 60 2                      # Create topics for 60s at 2/second
  $0 benchmark                         # Run performance benchmark

EOF
}

# Parse command line arguments
COMMAND="single"
while [[ $# -gt 0 ]]; do
    case $1 in
        single|multiple|parallel|stress|benchmark|help)
            COMMAND="$1"
            shift
            ;;
        --bootstrap-servers)
            BOOTSTRAP_SERVERS="$2"
            shift 2
            ;;
        --prefix)
            TOPIC_PREFIX="$2"
            shift 2
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            # Collect numeric arguments for commands
            if [[ "$1" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                ARGS+=("$1")
            else
                echo "Unknown option: $1"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Verify Kafka connectivity
echo -e "${BLUE}🔍 Verifying Kafka connectivity...${NC}"
if ! ./kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" --list &>/dev/null; then
    echo -e "${RED}❌ Cannot connect to Kafka at $BOOTSTRAP_SERVERS${NC} with $SSL_CONFIG_PATH"
    echo "Please check your bootstrap servers configuration."
    exit 1
fi
echo -e "${GREEN}✅ Connected to Kafka at $BOOTSTRAP_SERVERS${NC}"
echo ""

# Execute command
case $COMMAND in
    single)
        create_uuid_topic "${ARGS[0]}" "${ARGS[1]}"
        ;;
    multiple)
        if [ ${#ARGS[@]} -eq 0 ]; then
            echo "Error: COUNT required for multiple command"
            show_usage
            exit 1
        fi
        create_multiple_topics "${ARGS[0]}" "${ARGS[1]}" "${ARGS[2]}" "${ARGS[3]}"
        ;;
    parallel)
        if [ ${#ARGS[@]} -eq 0 ]; then
            echo "Error: COUNT required for parallel command"
            show_usage
            exit 1
        fi
        create_parallel_topics "${ARGS[0]}" "${ARGS[1]}" "${ARGS[2]}" "${ARGS[3]}"
        ;;
    stress)
        if [ ${#ARGS[@]} -lt 2 ]; then
            echo "Error: DURATION and RATE required for stress command"
            show_usage
            exit 1
        fi
        stress_test "${ARGS[0]}" "${ARGS[1]}" "${ARGS[2]}" "${ARGS[3]}"
        ;;
    benchmark)
        benchmark "${ARGS[0]}" "${ARGS[1]}"
        ;;
    help)
        show_usage
        ;;
    *)
        echo "Unknown command: $COMMAND"
        show_usage
        exit 1
        ;;
esac