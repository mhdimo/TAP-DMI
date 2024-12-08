FROM wurstmeister/kafka:latest

# Copy custom configurations if needed
COPY config/server.properties /opt/kafka/config/

# Environment variables can be set in docker-compose.yml
ENV KAFKA_ADVERTISED_HOST_NAME=kafka \
    KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    KAFKA_CREATE_TOPICS="osu-chat-topic:1:1,osu-chat-sentiment:1:1"
