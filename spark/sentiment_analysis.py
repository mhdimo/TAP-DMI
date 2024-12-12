from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from textblob import TextBlob
import json

def analyze_sentiment(message):
    blob = TextBlob(message)
    return blob.sentiment.polarity

def main():
    spark = SparkSession.builder \
        .appName("IRC Sentiment Analysis") \
        .getOrCreate()

    # Define UDF for sentiment analysis
    sentiment_udf = udf(analyze_sentiment, FloatType())

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "irc-messages") \
        .option("startingOffsets", "earliest") \
        .load()

    # Convert the binary 'value' column to string
    df = df.selectExpr("CAST(value AS STRING) as json_str")

    # Parse JSON and extract fields
    schema = StructType([
        StructField("username", StringType()),
        StructField("message", StringType()),
        StructField("timestamp", StringType()),
        StructField("channel", StringType())
    ])

    df = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

    # Perform sentiment analysis
    df = df.withColumn("sentiment_score", sentiment_udf(col("message")))

    # Write to Elasticsearch
    query = df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "irc-sentiments/doc") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()