from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from sparknlp.annotator import *
from sparknlp.base import *
from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging
import signal
import sys

class SentimentAnalyzer:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        self.spark = SparkSession.builder \
            .appName("OsuChatSentiment") \
            .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0") \
            .getOrCreate()
            
        self.running = True
        self.pipeline = self.create_pipeline()
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'sentiment-analyzer',
            'auto.offset.reset': 'earliest'
        })
        self.producer = Producer({'bootstrap.servers': 'kafka:9092'})

    def create_pipeline(self):
        document = DocumentAssembler() \
            .setInputCol("message") \
            .setOutputCol("document")
            
        sentence = SentenceDetector() \
            .setInputCols(["document"]) \
            .setOutputCol("sentence")
            
        tokenizer = Tokenizer() \
            .setInputCols(["sentence"]) \
            .setOutputCol("token")
            
        # Using specific pretrained model for English sentiment analysis
        sentiment = SentimentDLModel.pretrained('sentimentdl_use_twitter', lang='en') \
            .setInputCols(["sentence"]) \
            .setOutputCol("sentiment")
            
        return Pipeline(stages=[
            document,
            sentence,
            tokenizer,
            sentiment
        ])

    def process_sentiment(self, sentiment_result):
        # Map sentiment labels to scores
        sentiment_map = {
            'positive': 1.0,
            'neutral': 0.0,
            'negative': -1.0
        }
        sentiment_label = sentiment_result.lower()
        return {
            'label': sentiment_label,
            'score': sentiment_map.get(sentiment_label, 0.0)
        }

    def analyze(self):
        self.consumer.subscribe(['chat-for-sentiment'])
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info('Reached end of partition')
                    else:
                        logging.error(f'Error: {msg.error()}')
                    continue

                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    # Create DataFrame with message
                    df = self.spark.createDataFrame([[data['message']]], ["message"])
                    
                    # Get sentiment
                    result = self.pipeline.fit(df).transform(df)
                    sentiment_result = result.select("sentiment.result").collect()[0][0][0]
                    
                    # Process sentiment
                    sentiment_analysis = self.process_sentiment(sentiment_result)
                    
                    # Add sentiment to data
                    data['sentiment'] = sentiment_analysis
                    
                    # Send enriched data
                    self.producer.produce(
                        'chat-analyzed',
                        key=data.get('username', '').encode('utf-8'),
                        value=json.dumps(data).encode('utf-8'),
                        callback=self.delivery_report
                    )
                    self.producer.poll(0)
                    
                except Exception as e:
                    logging.error(f'Error processing message: {e}')

        except KeyboardInterrupt:
            logging.info('Shutting down...')
        finally:
            self.shutdown()

    def delivery_report(self, err, msg):
        if err is not None:
            logging.error(f'Message delivery failed: {err}')
        else:
            logging.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def signal_handler(self, signum, frame):
        logging.info('Shutdown signal received')
        self.running = False

    def shutdown(self):
        self.consumer.close()
        self.producer.flush()
        self.spark.stop()

    def run(self):
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.analyze()

if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    analyzer.run()