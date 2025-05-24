import time
import logging
import sys
import traceback
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import uuid

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

CASSANDRA_HOSTS = ['cassandra']
MAX_RETRIES = 20
RETRY_INTERVAL = 5


def create_keyspace_and_tables():
    """Create keyspace and tables in Cassandra (retry on failure)"""
    auth_provider = None
    retries = 0
    while retries < MAX_RETRIES:
        try:
            cluster = Cluster(CASSANDRA_HOSTS, auth_provider=auth_provider)
            session = cluster.connect()
            logging.info("Connected to Cassandra")

            # Create keyspace if it doesn't exist
            session.execute("""
            CREATE KEYSPACE IF NOT EXISTS deals_keyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)
            session.set_keyspace("deals_keyspace")

            # Create tables with proper UUID type
            session.execute("""
                            CREATE TABLE IF NOT EXISTS deals
                            (
                                deal_id
                                text
                                PRIMARY
                                KEY,
                                user_id
                                text,
                                item
                                text,
                                retailer
                                text,
                                price
                                double,
                                discount
                                text,
                                url
                                text,
                                original_timestamp
                                text,
                                processed_timestamp
                                timestamp
                            )
                            """)

            session.execute("""
                            CREATE TABLE IF NOT EXISTS recommended_deals
                            (
                                recommendation_id
                                text
                                PRIMARY
                                KEY,
                                user_id
                                text,
                                deal_id
                                text,
                                recommendation_score
                                double,
                                recommendation_timestamp
                                timestamp
                            )
                            """)

            # Add index on user_id for faster lookups
            try:
                session.execute("""
                                CREATE INDEX IF NOT EXISTS deals_user_id_idx ON deals_keyspace.deals (user_id)
                                """)
                session.execute("""
                                CREATE INDEX IF NOT EXISTS recommended_user_id_idx ON deals_keyspace.recommended_deals (user_id)
                                """)
                logging.info("Indices created successfully")
            except Exception as e:
                logging.warning(f"Index creation warning (may already exist): {e}")

            logging.info("Keyspace and tables created successfully")
            cluster.shutdown()
            return True
        except Exception as e:
            logging.error("Error creating keyspace/tables:\n" + traceback.format_exc())
            retries += 1
            time.sleep(RETRY_INTERVAL)

    logging.error("Exceeded max retries. Exiting.")
    return False


def start_spark_streaming():
    """Initialize Spark Streaming job to process Kafka topic and write into Cassandra"""

    # Schema for the deals JSON payload - matching what scraper produces EXACTLY
    deals_schema = StructType([
        StructField("deal_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("item", StringType(), True),
        StructField("retailer", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", StringType(), True),
        StructField("timestamp", StringType(), True),  # ISO string from producer
        StructField("url", StringType(), True)
    ])

    try:
        # Initialize Spark Session with Cassandra connector config
        spark = SparkSession.builder \
            .appName("DealsStreamProcessor") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "10") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.cassandra.output.consistency.level", "ONE") \
            .getOrCreate()

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")

        logging.info("Spark session created successfully")

        # Read from Kafka topic 'deals'
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "deals") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.consumer.session.timeout.ms", "60000") \
            .option("kafka.consumer.heartbeat.interval.ms", "10000") \
            .option("kafka.consumer.request.timeout.ms", "70000") \
            .load()

        logging.info("Kafka source configured successfully")

        # Parse JSON and select fields with proper handling
        parsed_df = df.select(
            from_json(col("value").cast("string"), deals_schema).alias("data"),
            col("key").cast("string").alias("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            col("data.deal_id").alias("deal_id"),
            col("data.user_id").alias("user_id"),
            col("data.item").alias("item"),
            col("data.retailer").alias("retailer"),
            col("data.price").alias("price"),
            col("data.discount").alias("discount"),
            col("data.url").alias("url"),
            col("data.timestamp").alias("original_timestamp"),  # Keep original
            current_timestamp().alias("processed_timestamp"),
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset")
        ).filter(col("deal_id").isNotNull())  # Filter out malformed records

        # Add error handling for the stream
        def process_batch(batch_df, batch_id):
            try:
                count = batch_df.count()
                logging.info(f"=== PROCESSING BATCH {batch_id} ===")
                logging.info(f"Records in batch: {count}")

                if count > 0:
                    # Show what we're about to write
                    logging.info("Sample records:")
                    batch_df.select("deal_id", "user_id", "item", "retailer", "price").show(5, truncate=False)

                    # Prepare data for Cassandra (remove debug columns)
                    cassandra_df = batch_df.select(
                        "deal_id", "user_id", "item", "retailer", "price",
                        "discount", "url", "original_timestamp", "processed_timestamp"
                    )

                    # Write to Cassandra
                    logging.info(f"Writing batch {batch_id} with {count} records to Cassandra")
                    cassandra_df.write \
                        .format("org.apache.spark.sql.cassandra") \
                        .mode("append") \
                        .option("keyspace", "deals_keyspace") \
                        .option("table", "deals") \
                        .save()
                    logging.info(f"Successfully wrote batch {batch_id} to Cassandra")
                else:
                    logging.info(f"Batch {batch_id} is empty, skipping")

            except Exception as e:
                logging.error(f"Error processing batch {batch_id}: {str(e)}")
                logging.error(traceback.format_exc())

        # Write the stream into Cassandra using foreachBatch for better error handling
        query = parsed_df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .trigger(processingTime='30 seconds') \
            .start()

        logging.info("Spark Streaming job configured and started")

        # Keep the application running and log progress
        while query.isActive:
            query.awaitTermination(timeout=60)
            if query.isActive:
                logging.info("Streaming job is still active...")
            else:
                logging.warning("Streaming job stopped!")
                break

    except Exception as e:
        logging.error(f"Error in Spark Streaming: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    logging.info("Initializing Cassandra keyspace and tables...")
    if not create_keyspace_and_tables():
        sys.exit(1)

    # Add a delay to ensure Kafka is ready
    logging.info("Waiting for Kafka to be ready...")
    time.sleep(30)

    logging.info("Starting Spark Streaming application...")
    start_spark_streaming()