#!/bin/bash

# UUID Topic Creation Monitor
# Monitors Kafka topics with UUID patterns and measures creation performance

# Configuration
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SSL_CONFIG_PATH="${SSL_CONFIG_PATH:-../config/ssl.conf}"
PRODUCER_CONFIG_PATH="${PRODUCER_CONFIG_PATH:-../config/producer.properties}"
CONSUMER_CONFIG_PATH="${CONSUMER_CONFIG_PATH:-../config/consumer.properties}"

LOG_FILE="${LOG_FILE:-uuid_topic_monitor.log}"
METRICS_FILE="${METRICS_FILE:-uuid_topic_metrics.csv}"
CHECK_INTERVAL="${CHECK_INTERVAL:-2}"  # seconds between checks
MAX_TOPIC_AGE="${MAX_TOPIC_AGE:-3600}" # seconds (1 hour)
LATENCY_WARNING_THRESHOLD="${LATENCY_WARNING_THRESHOLD:-5000}"  # 5 seconds
LATENCY_CRITICAL_THRESHOLD="${LATENCY_CRITICAL_THRESHOLD:-10000}" # 10 seconds

# UUID patterns to match
UUID_PATTERN="[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
TOPIC_PREFIX="${TOPIC_PREFIX:-}"  # Optional prefix like "user-request-"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Initialize log files
init_monitoring() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting UUID Topic Monitor" >> "$LOG_FILE"
    
    # Create CSV header if file doesn't exist
    if [ ! -f "$METRICS_FILE" ]; then
        echo "timestamp,topic_name,creation_detected_time,verification_latency_ms,ready_latency_ms,partition_count,replication_factor,status,error_message" > "$METRICS_FILE"
    fi
    
    echo -e "${GREEN}✅ UUID Topic Monitor initialized${NC}"
    echo "Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
    echo "Topic prefix: $TOPIC_PREFIX"
    echo "Log File: $LOG_FILE"
    echo "Metrics File: $METRICS_FILE"
    echo "Check Interval: ${CHECK_INTERVAL}s"
    echo "UUID Pattern: $UUID_PATTERN"
    echo ""
}

# Function to check if string matches UUID pattern
is_uuid_topic() {
    local topic_name="$1"
    
    # Remove prefix if specified
    if [ -n "$TOPIC_PREFIX" ]; then
        topic_name="${topic_name#$TOPIC_PREFIX}"
    fi
    
    # Check if it matches UUID pattern
    if echo "$topic_name" | grep -qE "^${UUID_PATTERN}$"; then
        return 0
    else
        return 1
    fi
}

# Function to get topic details
get_topic_details() {
    local topic_name="$1"
    local details
    
    details=$(./kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH"\
              --describe --topic "$topic_name" 2>/dev/null)
    
    if [ $? -eq 0 ] && [ -n "$details" ]; then
        # Extract partition count and replication factor
        local partition_count=$(echo "$details" | grep "PartitionCount:" | sed 's/.*PartitionCount: *\([0-9]*\).*/\1/')
        local replication_factor=$(echo "$details" | grep "ReplicationFactor:" | sed 's/.*ReplicationFactor: *\([0-9]*\).*/\1/')
        
        echo "${partition_count:-unknown},${replication_factor:-unknown}"
        return 0
    else
        echo "unknown,unknown"
        return 1
    fi
}

# Function to verify topic is ready for operations
verify_topic_ready() {
    local topic_name="$1"
    local timeout_ms="${2:-30000}"  # 30 second default timeout
    local check_interval_ms=100
    local elapsed_ms=0
    
    while [ $elapsed_ms -lt $timeout_ms ]; do
        # Check if topic exists and has leader for all partitions
        local describe_output
        describe_output=$(./kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" \
                         --describe --topic "$topic_name" 2>/dev/null)
        
        if [ $? -eq 0 ] && [ -n "$describe_output" ]; then
            # Check if all partitions have leaders
            local partition_lines=$(echo "$describe_output" | grep "Topic:" | grep "Partition:")
            local partitions_with_leaders=0
            local total_partitions=0
            
            if [ -n "$partition_lines" ]; then
                while IFS= read -r line; do
                    if echo "$line" | grep -q "Leader:"; then
                        local leader=$(echo "$line" | sed 's/.*Leader: *\([0-9]*\).*/\1/')
                        if [ "$leader" != "-1" ] && [ "$leader" != "none" ]; then
                            ((partitions_with_leaders++))
                        fi
                        ((total_partitions++))
                    fi
                done <<< "$partition_lines"
                
                # If all partitions have leaders, topic is ready
                if [ $partitions_with_leaders -eq $total_partitions ] && [ $total_partitions -gt 0 ]; then
                    echo $elapsed_ms
                    return 0
                fi
            fi
        fi
        
        sleep 0.1
        elapsed_ms=$((elapsed_ms + check_interval_ms))
    done
    
    echo $elapsed_ms
    return 1
}

# Function to test topic produce/consume capability
test_topic_operations() {
    local topic_name="$1"
    local test_start=$(date +%s%3N)
    
    # Create a unique test message
    local test_message="health-check-$(date +%s)-$$"
    local test_key="test-key-$(date +%s)"
    
    # Test produce
    echo "$test_message" | ./kafka-console-producer.sh \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --producer.config  "$PRODUCER_CONFIG_PATH" \
        --topic "$topic_name" \
        --property "key.separator=:" \
        --property "parse.key=true" \
        --timeout 5000 2>/dev/null << EOF
$test_key:$test_message
EOF
    
    local produce_result=$?
    
    if [ $produce_result -eq 0 ]; then
        # Test consume - try to read back the message
        local consumed_message
        consumed_message=$(./kafka-console-consumer.sh \
                          --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                          --topic "$topic_name" \
                          --consumer.config "$CONSUMER_CONFIG_PATH"\
                          --from-beginning \
                          --max-messages 1 \
                          --timeout-ms 5000 2>/dev/null | head -1)
        local consume_result=$?
        local test_end=$(date +%s%3N)
        local operation_latency=$((test_end - test_start))
        
        if [ $consume_result -eq 0 ] && [ "$consumed_message" = "$test_message" ]; then
            echo "success,$operation_latency"
            return 0
        else
            echo "consume_failed,$operation_latency"
            return 1
        fi
    else
        local test_end=$(date +%s%3N)
        local operation_latency=$((test_end - test_start))
        echo "produce_failed,$operation_latency"
        return 1
    fi
}

# Function to monitor new topics
monitor_new_topics() {
    local previous_topics_file="/tmp/kafka_topics_previous_$$"
    local current_topics_file="/tmp/kafka_topics_current_$$"
    
    # Get initial topic list
    ./kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" --list 2>/dev/null | \
        grep -E "${TOPIC_PREFIX}${UUID_PATTERN}" > "$previous_topics_file" 2>/dev/null
    
    echo -e "${BLUE}🔍 Monitoring UUID topics (Pattern: ${TOPIC_PREFIX}${UUID_PATTERN})${NC}"
    echo "Press Ctrl+C to stop monitoring"
    echo ""
    
    while true; do
        local current_time=$(date '+%Y-%m-%d %H:%M:%S')
        local current_timestamp=$(date '+%Y-%m-%d %H:%M:%S.%3N')

        # Get current topic list
        ./kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" --list 2>/dev/null | \
            grep -E "${TOPIC_PREFIX}${UUID_PATTERN}" > "$current_topics_file" 2>/dev/null
        
        # Alternative method using diff
        if [ -f "$previous_topics_file" ]; then
            local new_topics
            new_topics=$(diff "$previous_topics_file" "$current_topics_file" 2>/dev/null | grep "^>" | sed 's/^> //')
            
            if [ -n "$new_topics" ]; then
                while IFS= read -r topic; do
                    if [ -n "$topic" ] && is_uuid_topic "$topic"; then
                        echo -e "${GREEN}[$current_time] 🆕 New UUID topic detected: $topic${NC}"
                        process_new_topic "$topic" "$current_timestamp"
                    fi
                done <<< "$new_topics"
            fi
        fi
        
        # Update previous topics list
        cp "$current_topics_file" "$previous_topics_file"
        
        sleep "$CHECK_INTERVAL"
    done
    
    # Cleanup
    rm -f "$previous_topics_file" "$current_topics_file"
}

# Function to process a newly detected topic
process_new_topic() {
    local topic_name="$1"
    local detection_time="$2"
    local processing_start=$(date +%s%3N)
    
    echo "  📊 Analyzing topic: $topic_name"
    
    # Get topic details
    local topic_details
    topic_details=$(get_topic_details "$topic_name")
    local partition_count=$(echo "$topic_details" | cut -d',' -f1)
    local replication_factor=$(echo "$topic_details" | cut -d',' -f2)
    
    echo "    Partitions: $partition_count, Replication Factor: $replication_factor"
    
    # Verify topic is ready
    echo "    🔍 Verifying topic readiness..."
    local verify_start=$(date +%s%3N)
    local ready_latency
    ready_latency=$(verify_topic_ready "$topic_name" 30000)
    local verify_result=$?
    local verify_end=$(date +%s%3N)
    local verification_latency=$((verify_end - verify_start))
    
    local status="unknown"
    local error_message=""
    
    if [ $verify_result -eq 0 ]; then
        echo -e "    ${GREEN}✅ Topic ready in ${ready_latency}ms${NC}"
        status="ready"
        
        # Test operations
        echo "    🧪 Testing produce/consume operations..."
        local operation_result
        operation_result=$(test_topic_operations "$topic_name")
        local operation_status=$(echo "$operation_result" | cut -d',' -f1)
        local operation_latency=$(echo "$operation_result" | cut -d',' -f2)
        
        if [ "$operation_status" = "success" ]; then
            echo -e "    ${GREEN}✅ Operations test passed (${operation_latency}ms)${NC}"
            status="operational"
        else
            echo -e "    ${YELLOW}⚠️  Operations test failed: $operation_status (${operation_latency}ms)${NC}"
            status="ready_not_operational"
            error_message="$operation_status"
        fi
        
    else
        echo -e "    ${RED}❌ Topic not ready within timeout (${verification_latency}ms)${NC}"
        status="not_ready"
        error_message="verification_timeout"
    fi
    
    # Log metrics
    local processing_end=$(date +%s%3N)
    local total_processing_time=$((processing_end - processing_start))
    
    # Log to CSV
    echo "$detection_time,$topic_name,$detection_time,$verification_latency,$ready_latency,$partition_count,$replication_factor,$status,$error_message" >> "$METRICS_FILE"
    
    # Log to main log file
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Topic: $topic_name, Status: $status, VerifyLatency: ${verification_latency}ms, ReadyLatency: ${ready_latency}ms" >> "$LOG_FILE"
    
    # Performance alerts
    if [ $verification_latency -gt $LATENCY_CRITICAL_THRESHOLD ]; then
        echo -e "    ${RED}🚨 CRITICAL: Very high verification latency (${verification_latency}ms > ${LATENCY_CRITICAL_THRESHOLD}ms)${NC}"
    elif [ $verification_latency -gt $LATENCY_WARNING_THRESHOLD ]; then
        echo -e "    ${YELLOW}⚠️  WARNING: High verification latency (${verification_latency}ms > ${LATENCY_WARNING_THRESHOLD}ms)${NC}"
    fi
    
    echo "    📈 Total processing time: ${total_processing_time}ms"
    echo ""
}

# Function to analyze existing UUID topics
analyze_existing_topics() {
    echo -e "${BLUE}🔍 Analyzing existing UUID topics...${NC}"
    
    local all_topics
    all_topics=$(./kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" --list 2>/dev/null)
    
    local uuid_topics=()
    while IFS= read -r topic; do
        if [ -n "$topic" ] && is_uuid_topic "$topic"; then
            uuid_topics+=("$topic")
        fi
    done <<< "$all_topics"
    
    echo "Found ${#uuid_topics[@]} existing UUID topics"
    
    if [ ${#uuid_topics[@]} -eq 0 ]; then
        echo "No existing UUID topics found"
        return 0
    fi
    
    # Analyze each topic
    for topic in "${uuid_topics[@]}"; do
        echo ""
        echo "Analyzing: $topic"
        
        # Get topic details
        local topic_details
        topic_details=$(get_topic_details "$topic")
        local partition_count=$(echo "$topic_details" | cut -d',' -f1)
        local replication_factor=$(echo "$topic_details" | cut -d',' -f2)
        
        echo "  Partitions: $partition_count"
        echo "  Replication Factor: $replication_factor"
        
        # Quick readiness check
        local ready_latency
        ready_latency=$(verify_topic_ready "$topic" 5000)  # 5 second timeout for existing topics
        local verify_result=$?
        
        if [ $verify_result -eq 0 ]; then
            echo -e "  Status: ${GREEN}Ready${NC} (${ready_latency}ms)"
        else
            echo -e "  Status: ${RED}Not Ready${NC} (timeout after 5s)"
        fi
    done
    
    echo ""
    echo -e "${GREEN}✅ Analysis complete${NC}"
    echo ""
}

# Function to generate statistics report
generate_statistics() {
    local time_window_hours="${1:-24}"  # Default 24 hours
    
    echo -e "${BLUE}📊 Generating statistics for last ${time_window_hours} hours...${NC}"
    
    if [ ! -f "$METRICS_FILE" ]; then
        echo "No metrics file found: $METRICS_FILE"
        return 1
    fi
    
    # Calculate time threshold
    local threshold_time=$(date -d "$time_window_hours hours ago" '+%Y-%m-%d %H:%M:%S')
    
    # Create temporary file with recent entries
    local temp_file="/tmp/recent_metrics_$$"
    awk -F',' -v threshold="$threshold_time" '$1 > threshold' "$METRICS_FILE" > "$temp_file"
    
    local total_topics=$(wc -l < "$temp_file")
    
    if [ $total_topics -eq 0 ]; then
        echo "No topic creation events found in the last $time_window_hours hours"
        rm -f "$temp_file"
        return 0
    fi
    
    echo ""
    echo "=== UUID Topic Creation Statistics ==="
    echo "Time Window: Last $time_window_hours hours"
    echo "Total Topics Created: $total_topics"
    
    # Status distribution
    echo ""
    echo "Status Distribution:"
    awk -F',' '{print $8}' "$temp_file" | sort | uniq -c | while read count status; do
        printf "  %-20s: %d (%.1f%%)\n" "$status" "$count" "$(echo "scale=1; $count * 100 / $total_topics" | bc -l)"
    done
    
    # Latency statistics (verification latency)
    echo ""
    echo "Verification Latency Statistics:"
    local latencies
    latencies=$(awk -F',' '$4 != "unknown" && $4 > 0 {print $4}' "$temp_file" | sort -n)
    
    if [ -n "$latencies" ]; then
        local count=$(echo "$latencies" | wc -l)
        local min=$(echo "$latencies" | head -1)
        local max=$(echo "$latencies" | tail -1)
        local avg=$(echo "$latencies" | awk '{sum += $1} END {if (NR > 0) print sum/NR; else print 0}')
        local median=$(echo "$latencies" | awk '{arr[NR]=$1} END {if (NR%2==1) print arr[(NR+1)/2]; else print (arr[NR/2]+arr[NR/2+1])/2}')
        
        echo "  Samples: $count"
        echo "  Min: ${min}ms"
        echo "  Max: ${max}ms"
        echo "  Average: $(printf "%.1f" "$avg")ms"
        echo "  Median: $(printf "%.1f" "$median")ms"
        
        # Calculate percentiles
        local p95_index=$(echo "scale=0; $count * 0.95" | bc | cut -d'.' -f1)
        local p99_index=$(echo "scale=0; $count * 0.99" | bc | cut -d'.' -f1)
        
        if [ $p95_index -gt 0 ]; then
            local p95=$(echo "$latencies" | sed -n "${p95_index}p")
            echo "  95th Percentile: ${p95}ms"
        fi
        
        if [ $p99_index -gt 0 ]; then
            local p99=$(echo "$latencies" | sed -n "${p99_index}p")
            echo "  99th Percentile: ${p99}ms"
        fi
    else
        echo "  No valid latency data found"
    fi
    
    # Partition distribution
    echo ""
    echo "Partition Count Distribution:"
    awk -F',' '$6 != "unknown" {print $6}' "$temp_file" | sort | uniq -c | while read count partitions; do
        printf "  %d partition(s): %d topics\n" "$partitions" "$count"
    done
    
    # Recent trend (last vs previous hour)
    if [ $time_window_hours -ge 2 ]; then
        local last_hour_time=$(date -d "1 hour ago" '+%Y-%m-%d %H:%M:%S')
        local last_hour_count=$(awk -F',' -v threshold="$last_hour_time" '$1 > threshold' "$temp_file" | wc -l)
        local previous_hour_count=$((total_topics - last_hour_count))
        
        echo ""
        echo "Recent Trend:"
        echo "  Last hour: $last_hour_count topics"
        echo "  Previous period: $previous_hour_count topics"
        
        if [ $previous_hour_count -gt 0 ]; then
            local trend_pct=$(echo "scale=1; ($last_hour_count - $previous_hour_count) * 100 / $previous_hour_count" | bc -l)
            if [ "$(echo "$trend_pct > 0" | bc -l)" -eq 1 ]; then
                echo -e "  Trend: ${GREEN}+${trend_pct}% increase${NC}"
            elif [ "$(echo "$trend_pct < 0" | bc -l)" -eq 1 ]; then
                echo -e "  Trend: ${RED}${trend_pct}% decrease${NC}"
            else
                echo "  Trend: No change"
            fi
        fi
    fi
    
    rm -f "$temp_file"
    echo ""
}

# Function to clean up old topics (optional)
cleanup_old_topics() {
    local max_age_seconds="$1"
    local dry_run="${2:-true}"
    
    echo -e "${BLUE}🧹 Checking for old UUID topics (max age: ${max_age_seconds}s)...${NC}"
    
    local all_topics
    all_topics=$(./kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" --list 2>/dev/null)
    
    local old_topics=()
    local current_time=$(date +%s)
    
    while IFS= read -r topic; do
        if [ -n "$topic" ] && is_uuid_topic "$topic"; then
            # Try to extract timestamp from UUID topic name or use heuristic
            # This is challenging without creation time metadata
            # For now, we'll skip automatic cleanup and just report
            old_topics+=("$topic")
        fi
    done <<< "$all_topics"
    
    echo "Found ${#old_topics[@]} UUID topics (manual review recommended)"
    
    if [ ${#old_topics[@]} -gt 0 ]; then
        echo "Topics found:"
        for topic in "${old_topics[@]}"; do
            echo "  - $topic"
        done
        
        if [ "$dry_run" = "false" ]; then
            echo -e "${YELLOW}⚠️  Automatic cleanup disabled for safety${NC}"
            echo "To delete topics manually, use:"
            echo "./kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --command-config "$SSL_CONFIG_PATH" --delete --topic TOPIC_NAME"
        fi
    fi
    
    echo ""
}

# Function to show usage
show_usage() {
    cat << EOF
UUID Topic Creation Monitor

USAGE:
  $0 [COMMAND] [OPTIONS]

COMMANDS:
  monitor          Start real-time monitoring of new UUID topics (default)
  analyze          Analyze existing UUID topics
  stats [HOURS]    Generate statistics report (default: 24 hours)
  cleanup [AGE]    Check for old topics (dry run only)
  help             Show this help message

OPTIONS:
  --bootstrap-servers SERVER    Kafka bootstrap servers (default: localhost:9092)
  --prefix PREFIX              Topic prefix to match (default: none)
  --interval SECONDS           Check interval in seconds (default: 2)
  --log-file FILE              Log file path (default: uuid_topic_monitor.log)
  --metrics-file FILE          Metrics CSV file (default: uuid_topic_metrics.csv)

ENVIRONMENT VARIABLES:
  KAFKA_BOOTSTRAP_SERVERS      Bootstrap servers
  SSL_CONFIG_PATH              SSL configuration path
  PRODUCER_CONFIG_PATH         Producer configuration path
  CONSUMER_CONFIG_PATH         Consumer configuration path
  TOPIC_PREFIX                 Topic prefix
  CHECK_INTERVAL              Check interval in seconds
  LOG_FILE                    Log file path
  METRICS_FILE                Metrics file path


EXAMPLES:
  $0 monitor                                    # Start monitoring
  $0 monitor --prefix "user-request-"          # Monitor with prefix
  $0 analyze                                    # Analyze existing topics
  $0 stats 12                                   # Show stats for last 12 hours
  $0 cleanup 3600                              # Check for topics older than 1 hour

EOF
}

# Signal handlers
cleanup_on_exit() {
    echo ""
    echo -e "${YELLOW}🛑 Monitoring stopped${NC}"
    exit 0
}

# Set up signal handlers
trap cleanup_on_exit SIGINT SIGTERM

# Parse command line arguments
COMMAND="monitor"
while [[ $# -gt 0 ]]; do
    case $1 in
        monitor|analyze|stats|cleanup|help)
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
        --interval)
            CHECK_INTERVAL="$2"
            shift 2
            ;;
        --log-file)
            LOG_FILE="$2"
            shift 2
            ;;
        --metrics-file)
            METRICS_FILE="$2"
            shift 2
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            if [[ "$1" =~ ^[0-9]+$ ]]; then
                # Numeric argument for stats or cleanup
                COMMAND_ARG="$1"
            else
                echo "Unknown option: $1"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Main execution
case $COMMAND in
    monitor)
        init_monitoring
        monitor_new_topics
        ;;
    analyze)
        init_monitoring
        analyze_existing_topics
        ;;
    stats)
        generate_statistics "${COMMAND_ARG:-24}"
        ;;
    cleanup)
        cleanup_old_topics "${COMMAND_ARG:-$MAX_TOPIC_AGE}" true
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