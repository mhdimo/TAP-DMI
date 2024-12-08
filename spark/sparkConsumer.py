# kafka_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from textblob import TextBlob
import logging

# Schema matching our Kafka messages
KAFKA_SCHEMA = StructType([
    StructField("username", StringType(), True),
    StructField("message", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("channel", StringType(), True)
])


def analyze_sentiment(text):
    return TextBlob(text).sentiment.polarity


def create_spark_session():
    return (SparkSession.builder
            .appName("OsuChatSentiment")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                    "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,"
                    "org.scala-lang:scala-library:2.12.15")  # Added Scala library
            .config("spark.driver.extraClassPath", "C:/spark-3.5.3-bin-hadoop3/jars/*")  # Add local jars
            .master("local[*]")
            .getOrCreate())


def process_batch(batch_df, _):
    if not batch_df.isEmpty():
        # Register UDF for sentiment analysis
        sentiment_udf = spark.udf.register("sentiment", analyze_sentiment)

        # Apply sentiment analysis and prepare for ES
        result = (batch_df
                  .withColumn("sentiment", sentiment_udf(col("message")))
                  .withColumn("processed_at", current_timestamp()))

        # Write to Elasticsearch
        (result.write
         .format("org.elasticsearch.spark.sql")
         .option("es.nodes", "localhost")
         .option("es.port", "9200")
         .option("es.resource", "osu-chat-sentiment")
         .mode("append")
         .save())

        logging.info(f"Processed batch with {result.count()} messages")


def main():
    global spark
    spark = create_spark_session()

    # Create streaming DataFrame
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "osu-chat-topic")
          .option("startingOffsets", "latest")
          .load())

    # Parse JSON messages
    parsed = df.select(from_json(
        col("value").cast("string"),
        KAFKA_SCHEMA
    ).alias("data")).select("data.*")

    # Process stream
    query = (parsed.writeStream
             .foreachBatch(process_batch)
             .outputMode("update")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Shutting down stream processor")
    except Exception as e:
        logging.error(f"Fatal error: {e}")