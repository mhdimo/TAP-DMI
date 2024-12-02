from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging
import signal
import sys
from datetime import datetime

class ChatStreamProcessor:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'chat-processor',
            'auto.offset.reset': 'earliest'
        })
        self.producer = Producer({'bootstrap.servers': 'kafka:9092'})
        self.running = True
        
    def process(self):
        self.consumer.subscribe(['osu-chat-raw'])
        
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
                    data = json.loads(msg.value().decode('utf-8'))
                    enriched_data = self.enrich_message(data)
                    
                    # Send to different destinations
                    self.producer.produce(
                        'chat-for-storage',
                        key=data.get('username', '').encode('utf-8'),
                        value=json.dumps(enriched_data).encode('utf-8'),
                        callback=self.delivery_report
                    )
                    self.producer.produce(
                        'chat-for-sentiment',
                        key=data.get('username', '').encode('utf-8'),
                        value=json.dumps(enriched_data).encode('utf-8'),
                        callback=self.delivery_report
                    )
                    self.producer.poll(0)
                    
                except Exception as e:
                    logging.error(f"Processing error: {e}")
                    
        except KeyboardInterrupt:
            logging.info("Shutting down...")
        finally:
            self.shutdown()

    def enrich_message(self, data):
        data['processed_at'] = datetime.utcnow().isoformat()
        data['message_length'] = len(data['message'])
        data['hour_of_day'] = datetime.fromisoformat(data['timestamp']).hour
        return data
        
    def delivery_report(self, err, msg):
        if err is not None:
            logging.error(f'Message delivery failed: {err}')
        else:
            logging.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            
    def signal_handler(self, signum, frame):
        logging.info("Shutdown signal received")
        self.running = False
        
    def shutdown(self):
        self.consumer.close()
        self.producer.flush()
        
    def run(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.process()

if __name__ == "__main__":
    processor = ChatStreamProcessor()
    processor.run()