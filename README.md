# Make scripts executable
chmod +x uuid_topic_monitor.sh
chmod +x uuid_topic_test.sh
chmod +x uuid_topic_health.sh
chmod +x uuid_topic_alerts.sh

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS="your-kafka-server:9092"
export TOPIC_PREFIX="user-request-"  # if your UUIDs have a prefix

# Start monitoring UUID topics
./uuid_topic_monitor.sh monitor


# Create a single UUID topic
./uuid_topic_test.sh single

# Create 10 topics sequentially
./uuid_topic_test.sh multiple 10

# Create 20 topics in parallel (max 5 at once)
./uuid_topic_test.sh parallel 20 3 1 5

# Stress test: create topics for 60 seconds at 2 topics/second
./uuid_topic_test.sh stress 60 2

# Run performance benchmark
./uuid_topic_test.sh benchmark


# Check health for last 15 minutes
./uuid_topic_health.sh health

# Check health for last 30 minutes
./uuid_topic_health.sh health 30

# Show topic creation rate for last 4 hours
./uuid_topic_health.sh rate 4

# Continuous health monitoring (check every 5 minutes)
./uuid_topic_health.sh continuous 300 15


# Setup complete monitoring infrastructure
sudo ./uuid_topic_alerts.sh setup

# Edit configuration
sudo vi /path/to/uuid_topic_alerts.conf

# Start services
sudo ./uuid_topic_alerts.sh start

# Test alerts
./uuid_topic_alerts.sh test

# Check status
./uuid_topic_alerts.sh status



🆕 New UUID topic detected: user-request-123e4567-e89b-12d3-a456-426614174000
📊 Analyzing topic: user-request-123e4567-e89b-12d3-a456-426614174000
    Partitions: 3, Replication Factor: 1
    🔍 Verifying topic readiness...
    ✅ Topic ready in 1247ms
    🧪 Testing produce/consume operations...
    ✅ Operations test passed (89ms)
    📈 Total processing time: 1425ms


🏥 UUID Topic Creation Health Check
=== Summary ===
Total Topics: 45
Successful: 43
Failed: 2
Success Rate: 95.6%
Error Rate: 4.4%

=== Latency Statistics ===
Average: 1234ms
Maximum: 3456ms
95th Percentile: 2890ms

Status: HEALTHY ✅    
