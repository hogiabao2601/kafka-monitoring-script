#!/bin/bash

# UUID Topic Health Check Script
# Quick health check for UUID topic creation performance

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SSL_CONFIG_PATH="${SSL_CONFIG_PATH:-../config/ssl.conf}"
PRODUCER_CONFIG_PATH="${PRODUCER_CONFIG_PATH:-../config/producer.properties}"
CONSUMER_CONFIG_PATH="${CONSUMER_CONFIG_PATH:-../config/consumer.properties}"
METRICS_FILE="${METRICS_FILE:-uuid_topic_metrics.csv}"
ALERT_EMAIL="${ALERT_EMAIL:-ops@company.com}"

# Thresholds
LATENCY_WARNING=5000   # 5 seconds
LATENCY_CRITICAL=10000 # 10 seconds
ERROR_RATE_WARNING=10  # 10%
ERROR_RATE_CRITICAL=25 # 25%

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Function to safely check if a value is numeric and greater than threshold
is_numeric() {
    local value="$1"
    [[ "$value" =~ ^[0-9]+(\.[0-9]+)?$ ]]
}

is_greater_than() {
    local value="$1"
    local threshold="$2"
    
    # Set defaults if empty
    value="${value:-0}"
    threshold="${threshold:-0}"
    
    # Check if both values are numeric
    if ! is_numeric "$value" || ! is_numeric "$threshold"; then
        return 1
    fi
    
    # Use awk for comparison to handle decimals
    local result=$(awk "BEGIN {print ($value > $threshold) ? 1 : 0}")
    [ "$result" -eq 1 ]
}

# Function to safely check if a value is greater than or equal to threshold
is_greater_equal() {
    local value="$1"
    local threshold="$2"
    
    # Set defaults if empty
    value="${value:-0}"
    threshold="${threshold:-0}"
    
    # Check if both values are numeric
    if ! is_numeric "$value" || ! is_numeric "$threshold"; then
        return 1
    fi
    
    # Use awk for comparison to handle decimals
    local result=$(awk "BEGIN {print ($value >= $threshold) ? 1 : 0}")
    [ "$result" -eq 1 ]
}

# Debug function to show all variable values
debug_variables() {
    if [ "${DEBUG:-false}" = "true" ]; then
        echo "=== DEBUG: Variable Values ==="
        echo "LOG_FILE: [$LOG_FILE]"
        echo "TIME_WINDOW: [$TIME_WINDOW]"
        echo "total_topics: [$total_topics]"
        echo "ready_topics: [$ready_topics]"
        echo "failed_topics: [$failed_topics]"
        echo "creating_topics: [$creating_topics]"
        echo "success_rate: [$success_rate]"
        echo "error_rate: [$error_rate]"
        echo "avg_latency: [$avg_latency]"
        echo "max_latency: [$max_latency]"
        echo "p95_latency: [$p95_latency]"
        echo "LATENCY_WARNING: [$LATENCY_WARNING]"
        echo "LATENCY_CRITICAL: [$LATENCY_CRITICAL]"
        echo "ERROR_RATE_WARNING: [$ERROR_RATE_WARNING]"
        echo "ERROR_RATE_CRITICAL: [$ERROR_RATE_CRITICAL]"
        echo "=========================="
    fi
}

# Function to check recent topic creation health
check_topic_creation_health() {
    local time_window_minutes="${1:-15}"
    
    echo -e "${BLUE}🏥 UUID Topic Creation Health Check${NC}"
    echo "Time Window: Last $time_window_minutes minutes"
    echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    
    if [ ! -f "$METRICS_FILE" ]; then
        echo -e "${YELLOW}⚠️  No metrics file found: $METRICS_FILE${NC}"
        echo "Run the monitor script first to collect metrics."
        return 1
    fi
    
    # Calculate time threshold
    local threshold_time=$(date -d "$time_window_minutes minutes ago" '+%Y-%m-%d %H:%M:%S')
    
    # Get recent entries
    local recent_entries=$(awk -F',' -v threshold="$threshold_time" '$1 > threshold' "$METRICS_FILE")
    
    if [ -z "$recent_entries" ]; then
        echo -e "${YELLOW}⚠️  No recent topic creation events found${NC}"
        echo "This could indicate:"
        echo "  - No topics created recently (normal if low traffic)"
        echo "  - Monitoring not running"
        echo "  - Clock synchronization issues"
        return 0
    fi
    
    # Calculate statistics
    local total_topics=$(echo "$recent_entries" | wc -l)
    local ready_topics=$(echo "$recent_entries" | awk -F',' '$8=="ready" || $8=="operational"' | wc -l)
    local failed_topics=$(echo "$recent_entries" | awk -F',' '$8=="not_ready" || $8=="failed"' | wc -l)
    
    # Calculate success and error rates safely
    local success_rate=0
    local error_rate=0
    
    if [ "$total_topics" -gt 0 ] 2>/dev/null; then
        success_rate=$(awk "BEGIN {printf \"%.1f\", $ready_topics * 100 / $total_topics}")
        error_rate=$(awk "BEGIN {printf \"%.1f\", $failed_topics * 100 / $total_topics}")
    fi
    
    # Ensure rates are numeric
    success_rate=${success_rate:-0}
    error_rate=${error_rate:-0}
    
    # Calculate latency statistics
    local latencies=$(echo "$recent_entries" | awk -F',' '$4 != "unknown" && $4 > 0 {print $4}' | sort -n)
    local avg_latency=0
    local max_latency=0
    local p95_latency=0
    
    if [ -n "$latencies" ]; then
        avg_latency=$(echo "$latencies" | awk '{sum += $1} END {if (NR > 0) printf "%.0f", sum/NR; else print 0}')
        max_latency=$(echo "$latencies" | tail -1)
        
        local latency_count=$(echo "$latencies" | wc -l)
        local p95_index=$(echo "scale=0; $latency_count * 0.95" | bc | cut -d'.' -f1)
        if [ $p95_index -gt 0 ]; then
            p95_latency=$(echo "$latencies" | sed -n "${p95_index}p")
        fi
    fi
    
    # Display results
    echo "=== Summary ===" 
    echo "Total Topics: $total_topics"
    echo "Successful: $ready_topics"
    echo "Failed: $failed_topics"
    echo "Success Rate: ${success_rate}%"
    echo "Error Rate: ${error_rate}%"
    echo ""
    
    echo "=== Latency Statistics ==="
    echo "Average: ${avg_latency}ms"
    echo "Maximum: ${max_latency}ms"
    echo "95th Percentile: ${p95_latency}ms"
    echo ""
    
    # Health assessment
    local health_status="healthy"
    local alerts=()
    
    # Check error rate using safe comparison
    if is_greater_equal "$error_rate" "$ERROR_RATE_CRITICAL"; then
        health_status="critical"
        alerts+=("CRITICAL: Error rate is ${error_rate}% (threshold: ${ERROR_RATE_CRITICAL}%)")
    elif is_greater_equal "$error_rate" "$ERROR_RATE_WARNING"; then
        if [ "$health_status" = "healthy" ]; then
            health_status="warning"
        fi
        alerts+=("WARNING: Error rate is ${error_rate}% (threshold: ${ERROR_RATE_WARNING}%)")
    fi
    
    # Add debug output
    debug_variables
    
    # Check latency using safe comparison function
    if is_greater_than "$avg_latency" "$LATENCY_CRITICAL"; then
        health_status="critical"
        alerts+=("CRITICAL: Average latency is ${avg_latency}ms (threshold: ${LATENCY_CRITICAL}ms)")
    elif is_greater_than "$avg_latency" "$LATENCY_WARNING"; then
        if [ "$health_status" = "healthy" ]; then
            health_status="warning"
        fi
        alerts+=("WARNING: Average latency is ${avg_latency}ms (threshold: ${LATENCY_WARNING}ms)")
    fi
    
    if is_greater_than "$p95_latency" "$LATENCY_CRITICAL"; then
        health_status="critical"
        alerts+=("CRITICAL: 95th percentile latency is ${p95_latency}ms (threshold: ${LATENCY_CRITICAL}ms)")
    elif is_greater_than "$p95_latency" "$LATENCY_WARNING"; then
        if [ "$health_status" = "healthy" ]; then
            health_status="warning"
        fi
        alerts+=("WARNING: 95th percentile latency is ${p95_latency}ms (threshold: ${LATENCY_WARNING}ms)")
    fi
    
    # Display health status
    echo "=== Health Status ==="
    case $health_status in
        "healthy")
            echo -e "Status: ${GREEN}HEALTHY ✅${NC}"
            ;;
        "warning")
            echo -e "Status: ${YELLOW}WARNING ⚠️${NC}"
            ;;
        "critical")
            echo -e "Status: ${RED}CRITICAL 🚨${NC}"
            ;;
    esac
    
    # Display alerts
    if [ ${#alerts[@]} -gt 0 ]; then
        echo ""
        echo "Alerts:"
        for alert in "${alerts[@]}"; do
            if [[ $alert =~ ^CRITICAL ]]; then
                echo -e "  ${RED}$alert${NC}"
            else
                echo -e "  ${YELLOW}$alert${NC}"
            fi
        done
    fi
    
    echo ""
    
    # Send email alert if critical
    if [ "$health_status" = "critical" ] && [ -n "$ALERT_EMAIL" ]; then
        send_alert_email "$health_status" "$time_window_minutes" "$total_topics" "$error_rate" "$avg_latency" "${alerts[@]}"
    fi
    
    return $([ "$health_status" = "healthy" ] && echo 0 || echo 1)
}

# Function to send alert email
send_alert_email() {
    local status="$1"
    local time_window="$2"
    local total_topics="$3"
    local error_rate="$4"
    local avg_latency="$5"
    shift 5
    local alerts=("$@")
    
    local subject="Kafka UUID Topic Creation Alert - $status"
    
    cat << EOF | mail -s "$subject" "$ALERT_EMAIL"
Kafka UUID Topic Creation Health Alert

Status: $status
Time Window: Last $time_window minutes
Server: $(hostname)
Timestamp: $(date '+%Y-%m-%d %H:%M:%S')

Metrics:
- Total Topics: $total_topics
- Error Rate: $error_rate%
- Average Latency: ${avg_latency}ms

Alerts:
$(printf "%s\n" "${alerts[@]}")

Please investigate the Kafka cluster and topic creation performance.

Metrics file: $METRICS_FILE
Bootstrap servers: $KAFKA_BOOTSTRAP_SERVERS
EOF
    
    echo "Alert email sent to: $ALERT_EMAIL"
}

# Function to run continuous health monitoring
continuous_monitor() {
    local check_interval="${1:-300}"  # 5 minutes default
    local time_window="${2:-15}"      # 15 minutes default
    
    echo -e "${BLUE}🔄 Starting continuous health monitoring${NC}"
    echo "Check interval: ${check_interval}s"
    echo "Time window: ${time_window} minutes"
    echo "Press Ctrl+C to stop"
    echo ""
    
    while true; do
        check_topic_creation_health "$time_window"
        echo "Next check in ${check_interval}s..."
        echo "$(printf '=%.0s' {1..50})"
        echo ""
        sleep "$check_interval"
    done
}

# Function to show topic creation rate
show_creation_rate() {
    local time_window_hours="${1:-1}"
    
    echo -e "${BLUE}📈 Topic Creation Rate Analysis${NC}"
    echo "Time Window: Last $time_window_hours hour(s)"
    echo ""
    
    if [ ! -f "$METRICS_FILE" ]; then
        echo -e "${YELLOW}⚠️  No metrics file found: $METRICS_FILE${NC}"
        return 1
    fi
    
    local threshold_time=$(date -d "$time_window_hours hours ago" '+%Y-%m-%d %H:%M:%S')
    local recent_entries=$(awk -F',' -v threshold="$threshold_time" '$1 > threshold' "$METRICS_FILE")
    
    if [ -z "$recent_entries" ]; then
        echo "No topics created in the last $time_window_hours hour(s)"
        return 0
    fi
    
    local total_topics=$(echo "$recent_entries" | wc -l)
    local time_window_seconds=$((time_window_hours * 3600))
    # Calculate rate metrics safely
    local time_window_seconds=3600  # 1 hour
    local topics_per_hour=0
    local topics_per_minute=0
    
    if [ "$total_topics" -gt 0 ] 2>/dev/null; then
        topics_per_hour=$(awk "BEGIN {printf \"%.2f\", $total_topics * 3600 / $time_window_seconds}")
        topics_per_minute=$(awk "BEGIN {printf \"%.2f\", $topics_per_hour / 60}")
    fi
    
    # Ensure rates are numeric
    topics_per_hour=${topics_per_hour:-0}
    topics_per_minute=${topics_per_minute:-0}
    
    echo "Total Topics: $total_topics"
    echo "Rate: $topics_per_hour topics/hour"
    echo "Rate: $topics_per_minute topics/minute"
    echo ""
    
    # Show hourly breakdown
    echo "Hourly Breakdown:"
    echo "$recent_entries" | awk -F',' '{
        hour = substr($1, 1, 13)
        count[hour]++
    } END {
        for (h in count) {
            printf "  %s:00 - %d topics\n", h, count[h]
        }
    }' | sort
    
    echo ""
}

# Function to check Kafka cluster health
check_kafka_health() {
    echo -e "${BLUE}🔍 Kafka Cluster Health Check${NC}"
    echo ""
    
    # Test connectivity
    echo "1. Testing Kafka connectivity..."
    if ./kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" --list &>/dev/null; then
        echo -e "   ${GREEN}✅ Connected to Kafka${NC}"
    else
        echo -e "   ${RED}❌ Cannot connect to Kafka${NC}"
        return 1
    fi
    
    # Check broker count
    echo "2. Checking broker information..."
    local broker_info
    broker_info=$(./kafka-broker-api-versions.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" 2>/dev/null)
    
    if [ -n "$broker_info" ]; then
        local broker_count=$(echo "$broker_info" | grep -c "^[0-9]")
        echo -e "   ${GREEN}✅ $broker_count broker(s) available${NC}"
    else
        echo -e "   ${YELLOW}⚠️  Could not retrieve broker information${NC}"
    fi
    
    # Test topic creation
    echo "3. Testing topic creation capability..."
    local test_topic="health-check-$(date +%s)"
    
    if ./kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" \
       --create --topic "$test_topic" --partitions 1 --replication-factor 1 &>/dev/null; then
        echo -e "   ${GREEN}✅ Topic creation works${NC}"
        
        # Clean up test topic
        ./kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --command-config "$SSL_CONFIG_PATH" \
                       --delete --topic "$test_topic" &>/dev/null
    else
        echo -e "   ${RED}❌ Topic creation failed${NC}"
        return 1
    fi
    
    echo ""
    echo -e "${GREEN}✅ Kafka cluster health check passed${NC}"
    echo ""
}

# Function to show usage
show_usage() {
    cat << EOF
UUID Topic Health Check Script

USAGE:
  $0 [COMMAND] [OPTIONS]

COMMANDS:
  health [MINUTES]           Run health check for last N minutes (default: 15)
  rate [HOURS]              Show topic creation rate for last N hours (default: 1)
  kafka                     Check Kafka cluster health
  continuous [INTERVAL] [WINDOW]  Run continuous monitoring
  help                      Show this help message

OPTIONS:
  --bootstrap-servers SERVER    Kafka bootstrap servers
  --metrics-file FILE           Metrics CSV file path
  --alert-email EMAIL           Email for alerts

ENVIRONMENT VARIABLES:
  KAFKA_BOOTSTRAP_SERVERS       Bootstrap servers
  METRICS_FILE                  Metrics file path
  ALERT_EMAIL                   Alert email address

EXAMPLES:
  $0 health                     # Check last 15 minutes
  $0 health 30                  # Check last 30 minutes
  $0 rate 4                     # Show rate for last 4 hours
  $0 kafka                      # Check Kafka health
  $0 continuous 600 20          # Monitor every 10 min, 20 min window

THRESHOLDS:
  Latency Warning: ${LATENCY_WARNING}ms
  Latency Critical: ${LATENCY_CRITICAL}ms
  Error Rate Warning: ${ERROR_RATE_WARNING}%
  Error Rate Critical: ${ERROR_RATE_CRITICAL}%

EOF
}

# Parse command line arguments
COMMAND="health"
COMMAND_ARG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        health|rate|kafka|continuous|help)
            COMMAND="$1"
            shift
            ;;
        --bootstrap-servers)
            KAFKA_BOOTSTRAP_SERVERS="$2"
            shift 2
            ;;
        --metrics-file)
            METRICS_FILE="$2"
            shift 2
            ;;
        --alert-email)
            ALERT_EMAIL="$2"
            shift 2
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            if [[ "$1" =~ ^[0-9]+$ ]]; then
                if [ -z "$COMMAND_ARG" ]; then
                    COMMAND_ARG="$1"
                else
                    COMMAND_ARG2="$1"
                fi
            else
                echo "Unknown option: $1"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Signal handler for continuous monitoring
cleanup_on_exit() {
    echo ""
    echo -e "${YELLOW}🛑 Health monitoring stopped${NC}"
    exit 0
}

trap cleanup_on_exit SIGINT SIGTERM

# Execute command
case $COMMAND in
    health)
        check_topic_creation_health "${COMMAND_ARG:-15}"
        ;;
    rate)
        show_creation_rate "${COMMAND_ARG:-1}"
        ;;
    kafka)
        check_kafka_health
        ;;
    continuous)
        continuous_monitor "${COMMAND_ARG:-300}" "${COMMAND_ARG2:-15}"
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