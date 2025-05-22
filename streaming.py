import time
import logging
import sys
import traceback
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
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

    try:
        # Initialize Spark Session with Cassandra connector config
        spark = SparkSession.builder \
            .appName("DealsStreamProcessor") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "100") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
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
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
            
        logging.info("Kafka source configured successfully")

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
        
        # Handle missing timestamp by using current time
        parsed_df = parsed_df.na.fill(current_timestamp(), subset=["processed_timestamp"])

        # Add error handling for the stream
        def process_batch(batch_df, batch_id):
            try:
                count = batch_df.count()
                if count > 0:
                    logging.info(f"Writing batch {batch_id} with {count} records to Cassandra")
                    batch_df.write \
                        .format("org.apache.spark.sql.cassandra") \
                        .mode("append") \
                        .option("keyspace", "deals_keyspace") \
                        .option("table", "deals") \
                        .save()
                    logging.info(f"Successfully wrote batch {batch_id} to Cassandra")
            except Exception as e:
                logging.error(f"Error processing batch {batch_id}: {str(e)}")

        # Write the stream into Cassandra using foreachBatch for better error handling
        query = parsed_df.writeStream \
            .foreachBatch(process_batch) \
            .start()

        logging.info("Spark Streaming job configured and started")
        query.awaitTermination()
    except Exception as e:
        logging.error(f"Error in Spark Streaming: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)

if __name__ == '__main__':
    logging.info("Initializing Cassandra keyspace and tables...")
    if not create_keyspace_and_tables():
        sys.exit(1)

    logging.info("Starting Spark Streaming application...")
    start_spark_streaming()