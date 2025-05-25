import time
import logging
import sys
import traceback
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

CASSANDRA_HOSTS = ['cassandra']
MAX_RETRIES = 20
RETRY_INTERVAL = 5

def create_keyspace_and_tables():
    """Create keyspace and tables in Cassandra (retry on failure)"""
    auth_provider = None
    retries = 0

    logging.info("=" * 60)
    logging.info(">>> Starting Cassandra Setup Sequence")
    logging.info("=" * 60)

    while retries < MAX_RETRIES:
        try:
            logging.info(">>> Connecting to Cassandra cluster...")
            cluster = Cluster(CASSANDRA_HOSTS, auth_provider=auth_provider, protocol_version=4)
            session = cluster.connect()
            logging.info(">>> Cassandra connection established.")

            session.execute("""
            CREATE KEYSPACE IF NOT EXISTS deals_keyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)
            session.set_keyspace("deals_keyspace")
            logging.info(">>> Keyspace 'deals_keyspace' ensured.")

            session.execute("""
                CREATE TABLE IF NOT EXISTS deals (
                    deal_id text PRIMARY KEY,
                    user_id text,
                    item text,
                    retailer text,
                    price double,
                    discount text,
                    url text,
                    original_timestamp text,
                    processed_timestamp timestamp
                )
            """)
            session.execute("""
                CREATE TABLE IF NOT EXISTS recommended_deals (
                    recommendation_id text PRIMARY KEY,
                    user_id text,
                    deal_id text,
                    recommendation_score double,
                    recommendation_timestamp timestamp
                )
            """)
            logging.info(">>> Tables 'deals' and 'recommended_deals' ensured.")

            try:
                session.execute("""
                    CREATE INDEX IF NOT EXISTS deals_user_id_idx ON deals_keyspace.deals (user_id)
                """)
                session.execute("""
                    CREATE INDEX IF NOT EXISTS recommended_user_id_idx ON deals_keyspace.recommended_deals (user_id)
                """)
                logging.info(">>> Indexes for 'user_id' ensured on both tables.")
            except Exception as e:
                logging.warning(f"Index creation warning (may already exist): {e}")

            cluster.shutdown()
            logging.info(">>> Cassandra setup complete.")
            return True

        except Exception as e:
            logging.error("!!! Error creating keyspace/tables:\n" + traceback.format_exc())
            retries += 1
            time.sleep(RETRY_INTERVAL)

    logging.error("!!! Failed to connect to Cassandra after multiple attempts.")
    return False

def start_spark_streaming():
    """Initialize Spark Streaming job to process Kafka topic and write into Cassandra"""

    logging.info("=" * 60)
    logging.info(">>> Starting Spark Streaming Initialization")
    logging.info("=" * 60)

    deals_schema = StructType([
        StructField("deal_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("item", StringType(), True),
        StructField("retailer", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("url", StringType(), True)
    ])

    try:
        logging.info(">>> Initializing Spark Session for stream processing...")
        spark = SparkSession.builder \
            .appName("DealsStreamProcessor") \
            .master("spark://spark-master:7077") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "5") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.cassandra.output.consistency.level", "ONE") \
            .config("spark.cassandra.connection.timeout_ms", "30000") \
            .config("spark.cassandra.read.timeout_ms", "30000") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.cores", "2") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logging.info(">>> Spark Session is live.")

        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "deals") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        logging.info(">>> Connected to Kafka topic 'deals'.")

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
            col("data.timestamp").alias("original_timestamp"),
            current_timestamp().alias("processed_timestamp"),
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset")
        ).filter(col("deal_id").isNotNull())

        logging.info(">>> Kafka payload successfully parsed into structured DataFrame.")

        def process_batch(batch_df, batch_id):
            try:
                logging.info("-" * 40)
                logging.info(f">>> Handling Streaming Batch {batch_id}")
                logging.info("-" * 40)

                count = batch_df.count()
                logging.info(f">>> Batch {batch_id}: Parsed {count} record(s).")

                if count > 0:
                    logging.info(">>> Sample records:")
                    batch_df.select("deal_id", "user_id", "item", "retailer", "price").show(5, truncate=False)

                    cassandra_df = batch_df.select(
                        "deal_id", "user_id", "item", "retailer", "price",
                        "discount", "url", "original_timestamp", "processed_timestamp"
                    )

                    logging.info(f">>> Writing batch {batch_id} to Cassandra...")
                    cassandra_df.write \
                        .format("org.apache.spark.sql.cassandra") \
                        .mode("append") \
                        .option("keyspace", "deals_keyspace") \
                        .option("table", "deals") \
                        .save()
                    logging.info(f">>> Batch {batch_id} successfully written to Cassandra.")
                else:
                    logging.info(f">>> Batch {batch_id} is empty, skipping.")

            except Exception as e:
                logging.error(f"!!! Error processing batch {batch_id}: {str(e)}")
                logging.error(traceback.format_exc())

        query = parsed_df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .trigger(processingTime='30 seconds') \
            .start()

        logging.info(">>> Spark Streaming job configured and started")

        while query.isActive:
            query.awaitTermination(timeout=60)
            if query.isActive:
                logging.info(">>> Streaming is running smoothly...")
            else:
                logging.warning("!!! Streaming query unexpectedly stopped.")

    except Exception as e:
        logging.error(f"!!! Error in Spark Streaming: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)

if __name__ == '__main__':
    logging.info(">>> Initializing Cassandra keyspace and tables...")
    if not create_keyspace_and_tables():
        sys.exit(1)

    logging.info(">>> Waiting for Kafka to be ready...")
    time.sleep(30)

    logging.info(">>> Starting Spark Streaming application...")
    start_spark_streaming()
