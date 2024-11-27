import irc.client
import json
import confluent_kafka
from datetime import datetime
import os
from dotenv import load_dotenv

# Load variables
load_dotenv()
IRC_SERVER = "irc.ppy.sh"
IRC_PORT = 6667
IRC_USERNAME = os.getenv("IRC_USERNAME")
IRC_PASSWORD = os.getenv("IRC_PASSWORD")
IRC_CHANNEL = "#osu"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "osu-chat-raw")

# Kafka Producer Configuration
kafka_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "irc-producer",
}
producer = Producer(kafka_config)

# Handle IRC messages
def on_message(connection, event):
    try:
        username = event.source.split("!")[0]
        message = event.arguments[0]
        timestamp = datetime.utcnow().isoformat()

        payload = {
            "username": username,
            "message": message,
            "timestamp": timestamp,
        }

        producer.produce(KAFKA_TOPIC, json.dumps(payload))
        producer.flush()

        print(f"Sent to Kafka: {payload}")

    except Exception as e:
        print(f"Error processing message: {e}")

# Function to handle IRC connection
def on_connect(connection, event):
    print(f"Connected to {IRC_SERVER}, joining channel {IRC_CHANNEL}")
    connection.join(IRC_CHANNEL)

# Main function
def main():
    client = irc.client.Reactor()

    try:
        connection = client.server().connect(IRC_SERVER, IRC_PORT, IRC_USERNAME, password=IRC_PASSWORD)
        connection.add_global_handler("welcome", on_connect)
        connection.add_global_handler("pubmsg", on_message)

        print("IRC client started. Listening for messages...")
        client.process_forever()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
