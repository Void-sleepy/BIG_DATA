import time
import logging
import sys
import traceback
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

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

            # Create tables
            session.execute("""
            CREATE TABLE IF NOT EXISTS deals (
                deal_id uuid PRIMARY KEY,
                user_id text,
                item text,
                retailer text,
                price float,
                discount float,
                url text,
                processed_timestamp timestamp
            )
            """)
            session.execute("""
            CREATE TABLE IF NOT EXISTS recommended_deals (
                recommendation_id uuid PRIMARY KEY,
                user_id text,
                deal_id uuid,
                recommendation_score float,
                recommendation_timestamp timestamp
            )
            """)
            logging.info("Keyspace and tables created successfully")
            return True
        except Exception as e:
            logging.error("Error creating keyspace/tables:\n" + traceback.format_exc())
            retries += 1
            time.sleep(RETRY_INTERVAL)

    logging.error("Exceeded max retries. Exiting.")
    return False

def start_spark_streaming():
    """Initialize Spark Streaming job to process Kafka topic and write into Cassandra"""

    # Schema for the deals JSON payload
    deals_schema = StructType([
        StructField("deal_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("item", StringType(), True),
        StructField("retailer", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("url", StringType(), True)
    ])

    # Initialize Spark Session with Cassandra connector config
    spark = SparkSession.builder \
        .appName("DealsStreamProcessor") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .getOrCreate()

    # Read from Kafka topic 'deals'
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "deals") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON and select fields
    parsed_df = df.select(
        from_json(col("value").cast("string"), deals_schema).alias("data")
    ).select(
        col("data.deal_id"),
        col("data.user_id"),
        col("data.item"),
        col("data.retailer"),
        col("data.price"),
        col("data.discount"),
        col("data.url"),
        col("data.timestamp").cast(TimestampType()).alias("processed_timestamp")
    )

    # Write the stream into Cassandra
    query = parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .option("keyspace", "deals_keyspace") \
        .option("table", "deals") \
        .start()

    logging.info("Spark Streaming job configured and started")
    query.awaitTermination()

if __name__ == '__main__':
    logging.info("Initializing Cassandra keyspace and tables...")
    if not create_keyspace_and_tables():
        sys.exit(1)

    logging.info("Starting Spark Streaming application...")
    start_spark_streaming()
