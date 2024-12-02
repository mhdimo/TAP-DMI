#!/bin/bash

# Create necessary directories
mkdir -p logs data

# Create Kafka topics
echo "Creating Kafka topics..."
docker-compose exec kafka kafka-topics.sh \
    --create --if-not-exists \
    --bootstrap-server kafka:9092 \
    --topic osu-chat-raw \
    --partitions 3 \
    --replication-factor 1

docker-compose exec kafka kafka-topics.sh \
    --create --if-not-exists \
    --bootstrap-server kafka:9092 \
    --topic chat-for-storage \
    --partitions 3 \
    --replication-factor 1

docker-compose exec kafka kafka-topics.sh \
    --create --if-not-exists \
    --bootstrap-server kafka:9092 \
    --topic chat-for-sentiment \
    --partitions 3 \
    --replication-factor 1

docker-compose exec kafka kafka-topics.sh \
    --create --if-not-exists \
    --bootstrap-server kafka:9092 \
    --topic chat-analyzed \
    --partitions 3 \
    --replication-factor 1

echo "Waiting for services to be ready..."
sleep 30

# Initialize Elasticsearch index
curl -X PUT "localhost:9200/_template/chat_messages" -H 'Content-Type: application/json' -d'{
  "index_patterns": ["chat_messages*"],
  "mappings": {
    "properties": {
      "timestamp": { "type": "date" },
      "username": { "type": "keyword" },
      "message": { "type": "text" },
      "sentiment_score": { "type": "float" }
    }
  }
}'

# Initialize Kibana dashboards
echo "Setting up Kibana dashboards..."
curl -X POST "localhost:5601/api/saved_objects/_import" \
    -H "kbn-xsrf: true" \
    --form file=@kibana/dashboards.ndjson

echo "Setup complete!"