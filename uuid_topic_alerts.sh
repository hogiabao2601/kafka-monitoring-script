#!/bin/bash

# UUID Topic Alerting Configuration Script
# Sets up monitoring and alerting for UUID topic creation

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITOR_SCRIPT="$SCRIPT_DIR/uuid_topic_monitor.sh"
HEALTH_SCRIPT="$SCRIPT_DIR/uuid_topic_health.sh"

# Default configuration
DEFAULT_CONFIG_FILE="$SCRIPT_DIR/uuid_topic_alerts.conf"
LOG_DIR="${LOG_DIR:-/var/log/kafka-monitoring}"
SYSTEMD_DIR="/etc/systemd/system"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Function to create default configuration
create_default_config() {
    cat << 'EOF' > "$DEFAULT_CONFIG_FILE"
# UUID Topic Monitoring Configuration

# Kafka Settings
BOOTSTRAP_SERVERS="localhost:9092"
TOPIC_PREFIX="user-request-"

# Monitoring Settings
CHECK_INTERVAL=5                    # seconds between checks
HEALTH_CHECK_INTERVAL=300          # seconds between health checks (5 minutes)
HEALTH_TIME_WINDOW=15              # minutes to analyze for health checks

# Alerting Thresholds
LATENCY_WARNING_MS=5000            # 5 seconds
LATENCY_CRITICAL_MS=10000          # 10 seconds
ERROR_RATE_WARNING_PCT=10          # 10%
ERROR_RATE_CRITICAL_PCT=25         # 25%

# File Paths
LOG_DIR="/var/log/kafka-monitoring"
METRICS_FILE="$LOG_DIR/uuid_topic_metrics.csv"
MONITOR_LOG="$LOG_DIR/uuid_topic_monitor.log"
HEALTH_LOG="$LOG_DIR/uuid_topic_health.log"

# Email Alerting
ALERT_EMAIL="ops@company.com"
SMTP_SERVER="localhost"

# Slack Alerting (optional)
SLACK_WEBHOOK_URL=""
SLACK_CHANNEL="#kafka-alerts"

# PagerDuty (optional)
PAGERDUTY_INTEGRATION_KEY=""

# Monitoring Enabled
ENABLE_MONITORING=true
ENABLE_HEALTH_CHECKS=true
ENABLE_EMAIL_ALERTS=true
ENABLE_SLACK_ALERTS=false
ENABLE_PAGERDUTY=false
EOF

    echo -e "${GREEN}✅ Default configuration created: $DEFAULT_CONFIG_FILE${NC}"
    echo "Please edit the configuration file with your specific settings."
}

# Function to create systemd service for monitoring
create_monitoring_service() {
    local config_file="${1:-$DEFAULT_CONFIG_FILE}"
    
    if [ ! -f "$config_file" ]; then
        echo -e "${RED}❌ Configuration file not found: $config_file${NC}"
        return 1
    fi
    
    # Load configuration
    source "$config_file"
    
    # Create systemd service file
    local service_file="$SYSTEMD_DIR/uuid-topic-monitor.service"
    
    sudo tee "$service_file" > /dev/null << EOF
[Unit]
Description=UUID Topic Creation Monitor
After=network.target
Wants=network.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="KAFKA_BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS"
Environment="TOPIC_PREFIX=$TOPIC_PREFIX"
Environment="CHECK_INTERVAL=$CHECK_INTERVAL"
Environment="LOG_FILE=$MONITOR_LOG"
Environment="METRICS_FILE=$METRICS_FILE"
ExecStart=$MONITOR_SCRIPT monitor
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

    # Create systemd timer and service for health checks
    local health_service_file="$SYSTEMD_DIR/uuid-topic-health.service"
    local health_timer_file="$SYSTEMD_DIR/uuid-topic-health.timer"
    
    sudo tee "$health_service_file" > /dev/null << EOF
[Unit]
Description=UUID Topic Health Check
After=network.target

[Service]
Type=oneshot
User=kafka
Group=kafka
Environment="KAFKA_BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS"
Environment="METRICS_FILE=$METRICS_FILE"
Environment="ALERT_EMAIL=$ALERT_EMAIL"
ExecStart=$HEALTH_SCRIPT health $HEALTH_TIME_WINDOW
StandardOutput=journal
StandardError=journal
EOF

    sudo tee "$health_timer_file" > /dev/null << EOF
[Unit]
Description=Run UUID Topic Health Check
Requires=uuid-topic-health.service

[Timer]
OnBootSec=${HEALTH_CHECK_INTERVAL}sec
OnUnitActiveSec=${HEALTH_CHECK_INTERVAL}sec
Unit=uuid-topic-health.service

[Install]
WantedBy=timers.target
EOF

    echo -e "${GREEN}✅ Systemd services created${NC}"
    echo "Services:"
    echo "  - $service_file"
    echo "  - $health_service_file"
    echo "  - $health_timer_file"
}

# Function to create log directories
setup_logging() {
    local config_file="${1:-$DEFAULT_CONFIG_FILE}"
    
    if [ ! -f "$config_file" ]; then
        echo -e "${RED}❌ Configuration file not found: $config_file${NC}"
        return 1
    fi
    
    source "$config_file"
    
    # Create log directory
    sudo mkdir -p "$LOG_DIR"
    sudo chown kafka:kafka "$LOG_DIR"
    sudo chmod 755 "$LOG_DIR"
    
    # Create log files with proper permissions
    sudo touch "$METRICS_FILE" "$MONITOR_LOG" "$HEALTH_LOG"
    sudo chown kafka:kafka "$METRICS_FILE" "$MONITOR_LOG" "$HEALTH_LOG"
    sudo chmod 644 "$METRICS_FILE" "$MONITOR_LOG" "$HEALTH_LOG"
    
    # Set up log rotation
    local logrotate_file="/etc/logrotate.d/uuid-topic-monitoring"
    
    sudo tee "$logrotate_file" > /dev/null << EOF
$LOG_DIR/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 kafka kafka
    postrotate
        systemctl reload-or-restart uuid-topic-monitor.service
    endscript
}

$METRICS_FILE {
    weekly
    rotate 12
    compress
    delaycompress
    missingok
    notifempty
    create 644 kafka kafka
    copytruncate
}
EOF

    echo -e "${GREEN}✅ Logging setup complete${NC}"
    echo "Log directory: $LOG_DIR"
    echo "Log rotation configured: $logrotate_file"
}

# Function to create Slack notification script
create_slack_notifier() {
    local config_file="${1:-$DEFAULT_CONFIG_FILE}"
    local slack_script="$SCRIPT_DIR/send_slack_alert.sh"
    
    if [ ! -f "$config_file" ]; then
        echo -e "${RED}❌ Configuration file not found: $config_file${NC}"
        return 1
    fi
    
    source "$config_file"
    
    cat << 'EOF' > "$slack_script"
#!/bin/bash

# Slack Alert Sender for UUID Topic Monitoring

WEBHOOK_URL="$1"
CHANNEL="$2"
MESSAGE="$3"
STATUS="$4"

if [ -z "$WEBHOOK_URL" ] || [ -z "$MESSAGE" ]; then
    echo "Usage: $0 WEBHOOK_URL CHANNEL MESSAGE [STATUS]"
    exit 1
fi

# Set color based on status
case "$STATUS" in
    "critical")
        COLOR="danger"
        EMOJI="🚨"
        ;;
    "warning")
        COLOR="warning" 
        EMOJI="⚠️"
        ;;
    "good"|"healthy")
        COLOR="good"
        EMOJI="✅"
        ;;
    *)
        COLOR="warning"
        EMOJI="ℹ️"
        ;;
esac

# Create JSON payload
PAYLOAD=$(cat << EOP
{
    "channel": "$CHANNEL",
    "username": "Kafka Monitor",
    "icon_emoji": ":warning:",
    "attachments": [
        {
            "color": "$COLOR",
            "pretext": "$EMOJI Kafka UUID Topic Alert",
            "text": "$MESSAGE",
            "footer": "Kafka Monitoring",
            "ts": $(date +%s)
        }
    ]
}
EOP
)

# Send to Slack
curl -X POST -H 'Content-type: application/json' \
     --data "$PAYLOAD" \
     "$WEBHOOK_URL"

EOF

    chmod +x "$slack_script"
    echo -e "${GREEN}✅ Slack notifier created: $slack_script${NC}"
}

# Function to create enhanced health check with alerting
create_enhanced_health_check() {
    local config_file="${1:-$DEFAULT_CONFIG_FILE}"
    local enhanced_script="$SCRIPT_DIR/enhanced_health_check.sh"
    
    if [ ! -f "$config_file" ]; then
        echo -e "${RED}❌ Configuration file not found: $config_file${NC}"
        return 1
    fi
    
    cat << 'EOF' > "$enhanced_script"
#!/bin/bash

# Enhanced Health Check with Multiple Alert Channels

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${CONFIG_FILE:-$SCRIPT_DIR/uuid_topic_alerts.conf}"

# Load configuration
if [ -f "$CONFIG_FILE" ]; then
    source "$CONFIG_FILE"
else
    echo "Configuration file not found: $CONFIG_FILE"
    exit 1
fi

# Function to send alerts through multiple channels
send_multi_channel_alert() {
    local status="$1"
    local message="$2"
    
    # Email alert
    if [ "$ENABLE_EMAIL_ALERTS" = "true" ] && [ -n "$ALERT_EMAIL" ]; then
        echo "$message" | mail -s "Kafka UUID Topic Alert - $status" "$ALERT_EMAIL"
        echo "Email alert sent to: $ALERT_EMAIL"
    fi
    
    # Slack alert
    if [ "$ENABLE_SLACK_ALERTS" = "true" ] && [ -n "$SLACK_WEBHOOK_URL" ]; then
        "$SCRIPT_DIR/send_slack_alert.sh" "$SLACK_WEBHOOK_URL" "$SLACK_CHANNEL" "$message" "$status"
        echo "Slack alert sent to: $SLACK_CHANNEL"
    fi
    
    # PagerDuty alert (for critical issues)
    if [ "$ENABLE_PAGERDUTY" = "true" ] && [ -n "$PAGERDUTY_INTEGRATION_KEY" ] && [ "$status" = "critical" ]; then
        send_pagerduty_alert "$message"
        echo "PagerDuty alert triggered"
    fi
}

# Function to send PagerDuty alert
send_pagerduty_alert() {
    local message="$1"
    
    local payload=$(cat << EOP
{
    "routing_key": "$PAGERDUTY_INTEGRATION_KEY",
    "event_action": "trigger",
    "payload": {
        "summary": "Kafka UUID Topic Creation Critical Alert",
        "source": "$(hostname)",
        "severity": "critical",
        "custom_details": {
            "message": "$message",
            "timestamp": "$(date -Iseconds)"
        }
    }
}
EOP
)

    curl -X POST \
         -H "Content-Type: application/json" \
         -d "$payload" \
         "https://events.pagerduty.com/v2/enqueue"
}

# Run the original health check
export KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVERS"
export METRICS_FILE="$METRICS_FILE"
export ALERT_EMAIL="$ALERT_EMAIL"

HEALTH_OUTPUT=$($SCRIPT_DIR/uuid_topic_health.sh health "$HEALTH_TIME_WINDOW" 2>&1)
HEALTH_EXIT_CODE=$?

# Extract key metrics from output
ERROR_RATE=$(echo "$HEALTH_OUTPUT" | grep "Error Rate:" | sed 's/.*Error Rate: *\([0-9.]*\)%.*/\1/')
AVG_LATENCY=$(echo "$HEALTH_OUTPUT" | grep "Average:" | sed 's/.*Average: *\([0-9]*\)ms.*/\1/')
STATUS=$(echo "$HEALTH_OUTPUT" | grep "Status:" | sed 's/.*Status: *\([A-Z]*\).*/\1/')

# Determine if we should send alerts
SHOULD_ALERT=false
ALERT_MESSAGE="Kafka UUID Topic Health Check Results:\n\n$HEALTH_OUTPUT"

if [ "$STATUS" = "CRITICAL" ]; then
    SHOULD_ALERT=true
    send_multi_channel_alert "critical" "$ALERT_MESSAGE"
elif [ "$STATUS" = "WARNING" ]; then
    SHOULD_ALERT=true
    send_multi_channel_alert "warning" "$ALERT_MESSAGE"
fi

# Log the results
echo "$(date '+%Y-%m-%d %H:%M:%S') - Health Check: Status=$STATUS, ErrorRate=$ERROR_RATE%, AvgLatency=${AVG_LATENCY}ms, Alert=$SHOULD_ALERT" >> "$HEALTH_LOG"

exit $HEALTH_EXIT_CODE
EOF

    chmod +x "$enhanced_script"
    echo -e "${GREEN}✅ Enhanced health check created: $enhanced_script${NC}"
}

# Function to start services
start_services() {
    echo -e "${BLUE}🚀 Starting UUID Topic monitoring services...${NC}"
    
    # Reload systemd
    sudo systemctl daemon-reload
    
    # Enable and start monitoring service
    sudo systemctl enable uuid-topic-monitor.service
    sudo systemctl start uuid-topic-monitor.service
    
    # Enable and start health check timer
    sudo systemctl enable uuid-topic-health.timer
    sudo systemctl start uuid-topic-health.timer
    
    echo -e "${GREEN}✅ Services started${NC}"
    
    # Show service status
    echo ""
    echo "Service Status:"
    sudo systemctl status uuid-topic-monitor.service --no-pager -l
    echo ""
    sudo systemctl status uuid-topic-health.timer --no-pager -l
}

# Function to stop services
stop_services() {
    echo -e "${BLUE}🛑 Stopping UUID Topic monitoring services...${NC}"
    
    sudo systemctl stop uuid-topic-monitor.service
    sudo systemctl stop uuid-topic-health.timer
    sudo systemctl disable uuid-topic-monitor.service
    sudo systemctl disable uuid-topic-health.timer
    
    echo -e "${GREEN}✅ Services stopped and disabled${NC}"
}

# Function to show service status
show_status() {
    echo -e "${BLUE}📊 UUID Topic Monitoring Status${NC}"
    echo ""
    
    echo "Service Status:"
    sudo systemctl is-active uuid-topic-monitor.service && echo "Monitor: Active" || echo "Monitor: Inactive"
    sudo systemctl is-active uuid-topic-health.timer && echo "Health Timer: Active" || echo "Health Timer: Inactive"
    echo ""
    
    echo "Recent Monitor Logs:"
    sudo journalctl -u uuid-topic-monitor.service --no-pager -l -n 10
    echo ""
    
    echo "Recent Health Logs:"
    sudo journalctl -u uuid-topic-health.service --no-pager -l -n 5
}

# Function to test alerting
test_alerts() {
    local config_file="${1:-$DEFAULT_CONFIG_FILE}"
    
    if [ ! -f "$config_file" ]; then
        echo -e "${RED}❌ Configuration file not found: $config_file${NC}"
        return 1
    fi
    
    source "$config_file"
    
    echo -e "${BLUE}🧪 Testing alert channels...${NC}"
    
    local test_message="Test alert from UUID Topic monitoring system at $(date)"
    
    # Test email
    if [ "$ENABLE_EMAIL_ALERTS" = "true" ] && [ -n "$ALERT_EMAIL" ]; then
        echo "Testing email alert to: $ALERT_EMAIL"
        echo "$test_message" | mail -s "Kafka Monitoring Test Alert" "$ALERT_EMAIL"
        echo -e "${GREEN}✅ Email test sent${NC}"
    else
        echo -e "${YELLOW}⚠️  Email alerts disabled or not configured${NC}"
    fi
    
    # Test Slack
    if [ "$ENABLE_SLACK_ALERTS" = "true" ] && [ -n "$SLACK_WEBHOOK_URL" ]; then
        echo "Testing Slack alert to: $SLACK_CHANNEL"
        if [ -f "$SCRIPT_DIR/send_slack_alert.sh" ]; then
            "$SCRIPT_DIR/send_slack_alert.sh" "$SLACK_WEBHOOK_URL" "$SLACK_CHANNEL" "$test_message" "good"
            echo -e "${GREEN}✅ Slack test sent${NC}"
        else
            echo -e "${RED}❌ Slack script not found${NC}"
        fi
    else
        echo -e "${YELLOW}⚠️  Slack alerts disabled or not configured${NC}"
    fi
    
    echo ""
}

# Function to show usage
show_usage() {
    cat << EOF
UUID Topic Alerting Configuration Script

USAGE:
  $0 [COMMAND] [CONFIG_FILE]

COMMANDS:
  setup [CONFIG]        Complete setup (config, logging, services)
  config               Create default configuration file
  services [CONFIG]    Create systemd services
  logging [CONFIG]     Setup logging and rotation
  slack [CONFIG]       Create Slack notification script
  enhanced [CONFIG]    Create enhanced health check
  start                Start monitoring services
  stop                 Stop monitoring services
  status               Show service status
  test [CONFIG]        Test alert channels
  help                 Show this help message

FILES:
  Default config: $DEFAULT_CONFIG_FILE

EXAMPLES:
  $0 setup                           # Complete setup with defaults
  $0 setup /etc/kafka/monitoring.conf # Setup with custom config
  $0 start                           # Start services
  $0 test                            # Test alerts
  $0 status                          # Check status

SETUP STEPS:
  1. Run '$0 config' to create configuration
  2. Edit configuration file with your settings
  3. Run '$0 setup' to complete installation
  4. Run '$0 start' to begin monitoring
  5. Run '$0 test' to verify alerts work

EOF
}

# Parse command line arguments
COMMAND="setup"
CONFIG_ARG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        setup|config|services|logging|slack|enhanced|start|stop|status|test|help)
            COMMAND="$1"
            shift
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            CONFIG_ARG="$1"
            shift
            ;;
    esac
done

# Set config file
CONFIG_FILE="${CONFIG_ARG:-$DEFAULT_CONFIG_FILE}"

# Execute command
case $COMMAND in
    setup)
        echo -e "${BLUE}🔧 Complete UUID Topic Monitoring Setup${NC}"
        echo ""
        create_default_config
        setup_logging "$CONFIG_FILE"
        create_monitoring_service "$CONFIG_FILE" 
        create_slack_notifier "$CONFIG_FILE"
        create_enhanced_health_check "$CONFIG_FILE"
        echo ""
        echo -e "${GREEN}✅ Setup complete!${NC}"
        echo "Next steps:"
        echo "1. Edit configuration: $CONFIG_FILE"
        echo "2. Start services: $0 start"
        echo "3. Test alerts: $0 test"
        ;;
    config)
        create_default_config
        ;;
    services)
        create_monitoring_service "$CONFIG_FILE"
        ;;
    logging)
        setup_logging "$CONFIG_FILE"
        ;;
    slack)
        create_slack_notifier "$CONFIG_FILE"
        
        ;;
    enhanced)
        create_enhanced_health_check "$CONFIG_FILE"
        ;;
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    status)
        show_status
        ;;
    test)
        test_alerts "$CONFIG_FILE"
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