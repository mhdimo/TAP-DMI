from elasticsearch import Elasticsearch
import psycopg2
from confluent_kafka import Consumer, KafkaError
import json
import logging
import signal
import sys
from datetime import datetime

class DatabaseConnector:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        self.es = Elasticsearch(['elasticsearch:9200'])
        self.pg_conn = psycopg2.connect(
            host="postgres",
            database="osu_chat",
            user="admin",
            password="admin123"
        )
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'storage-consumer',
            'auto.offset.reset': 'earliest'
        })
        self.running = True
        
    def initialize(self):
        self.create_postgres_schema()
        self.create_elasticsearch_index()
        
    def create_postgres_schema(self):
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100),
                    message TEXT,
                    timestamp TIMESTAMP,
                    processed_at TIMESTAMP,
                    message_length INTEGER,
                    hour_of_day INTEGER,
                    sentiment_label VARCHAR(20),
                    sentiment_score FLOAT
                )
            """)
        self.pg_conn.commit()
        
    def create_elasticsearch_index(self):
        if not self.es.indices.exists(index="chat_messages"):
            self.es.indices.create(index="chat_messages", body={
                "mappings": {
                    "properties": {
                        "username": {"type": "keyword"},
                        "message": {"type": "text"},
                        "timestamp": {"type": "date"},
                        "processed_at": {"type": "date"},
                        "message_length": {"type": "integer"},
                        "hour_of_day": {"type": "integer"},
                        "sentiment_label": {"type": "keyword"},
                        "sentiment_score": {"type": "float"}
                    }
                }
            })
            
    def store_message(self, message):
        try:
            # Store in PostgreSQL
            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO messages 
                    (username, message, timestamp, processed_at, message_length, 
                     hour_of_day, sentiment_label, sentiment_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    message['username'],
                    message['message'],
                    message['timestamp'],
                    message['processed_at'],
                    message['message_length'],
                    message['hour_of_day'],
                    message['sentiment']['label'],
                    message['sentiment']['score']
                ))
            self.pg_conn.commit()
            
            # Store in Elasticsearch
            self.es.index(index="chat_messages", document=message)
            
            logging.info(f"Stored message from {message['username']}")
            
        except Exception as e:
            logging.error(f"Error storing message: {e}")
            
    def signal_handler(self, signum, frame):
        logging.info("Shutdown signal received")
        self.running = False
        
    def run(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.initialize()
        self.consumer.subscribe(['chat-analyzed'])
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Consumer error: {msg.error()}")
                        break
                        
                try:
                    message = json.loads(msg.value().decode('utf-8'))
                    self.store_message(message)
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
                    
        except Exception as e:
            logging.error(f"Fatal error: {e}")
        finally:
            self.shutdown()
            
    def shutdown(self):
        self.consumer.close()
        self.pg_conn.close()
        
if __name__ == "__main__":
    connector = DatabaseConnector()
    connector.run()