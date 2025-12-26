#!/bin/bash

echo "🚀 Setting up HealthInsight project..."

# Install Python dependencies
echo "📦 Installing Python packages..."
pip install -r requirements.txt

# Start Docker containers
echo "🐳 Starting Docker containers..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start (60 seconds)..."
sleep 60

# Initialize Cassandra
echo "💾 Initializing Cassandra..."
docker exec -it cassandra-1 cqlsh -f /config/cassandra_setup.cql

# Initialize MongoDB replica set
echo "💾 Initializing MongoDB..."
docker exec -it mongodb-primary mongosh --eval "
  rs.initiate({
    _id: 'rs0',
    members: [
      {_id: 0, host: 'mongodb-primary:27017'},
      {_id: 1, host: 'mongodb-secondary:27017'}
    ]
  })
"

sleep 30

# Load MongoDB schema
docker exec -it mongodb-primary mongosh healthinsight < config/mongodb_setup.js

# Create Kafka topics
echo "📨 Creating Kafka topics..."
docker exec -it kafka bash /config/kafka_topics.sh

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Generate patient data: python data_generator/patient_generator.py"
echo "2. Start monitoring simulation: python data_generator/monitoring_simulator.py"
echo "3. Start Spark streaming: spark-submit analytics/spark_streaming_job.py"
