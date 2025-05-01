from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Schema for the deals

deals_schema = StructType([
    StructField("deal_id",        StringType(),   True),
    StructField("user_id",        StringType(),   True),
    StructField("item",           StringType(),   True),
    StructField("retailer",       StringType(),   True),
    StructField("price",          DoubleType(),   True),
    StructField("discount",       DoubleType(),   True),
    StructField("timestamp",      StringType(),   True),
    StructField("url",            StringType(),   True)
])

# Initialize Spark Session with Cassandra connector configuration
spark = SparkSession.builder \
    .appName("DealsStreamProcessor") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .getOrCreate()

# Read from Kafka
# Set appropriate options for Kafka source
# Notice: Kafka topic is 'deals' on localhost:9092

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

print("Spark Streaming job configured and started")

query.awaitTermination()