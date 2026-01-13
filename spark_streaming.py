"""
Spark Structured Streaming: Kafka to Cassandra Pipeline
Reads user data from Kafka, preprocesses it, and writes to Cassandra in real-time.
Automatically creates Cassandra keyspace and table if they don't exist.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, concat_ws, to_timestamp, 
    lower, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging
import sys
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
# Use localhost when running Spark locally on Windows
# Use kafka_job1:29092 when running inside Docker
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "random_users"

# Use localhost when running Spark locally on Windows
# Use "cassandra" when running inside Docker
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "user_data"
CASSANDRA_TABLE = "users"

# Use Windows path when running locally
# Use /tmp/spark_checkpoint when running in Docker/Linux
CHECKPOINT_DIR = "C:/tmp/spark_checkpoint"

# Cassandra credentials (if authentication is enabled)
CASSANDRA_USERNAME = None  # Set to your username if needed
CASSANDRA_PASSWORD = None  # Set to your password if needed

# ==================== CASSANDRA SETUP ====================
def setup_cassandra():
    """
    Create Cassandra keyspace and table if they don't exist.
    """
    logger.info("="*70)
    logger.info("Setting up Cassandra keyspace and table...")
    logger.info("="*70)
    
    try:
        # Connect to Cassandra
        logger.info(f"Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
        
        if CASSANDRA_USERNAME and CASSANDRA_PASSWORD:
            auth_provider = PlainTextAuthProvider(
                username=CASSANDRA_USERNAME,
                password=CASSANDRA_PASSWORD
            )
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
        else:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        
        session = cluster.connect()
        logger.info("✓ Connected to Cassandra successfully!")
        
        # Create keyspace
        logger.info(f"Creating keyspace '{CASSANDRA_KEYSPACE}' if not exists...")
        keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }}
        """
        session.execute(keyspace_query)
        logger.info(f"✓ Keyspace '{CASSANDRA_KEYSPACE}' is ready!")
        
        # Use the keyspace
        session.set_keyspace(CASSANDRA_KEYSPACE)
        
        # Create table
        logger.info(f"Creating table '{CASSANDRA_TABLE}' if not exists...")
        table_query = f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
            user_id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            title TEXT,
            gender TEXT,
            email TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            postcode TEXT,
            latitude DOUBLE,
            longitude DOUBLE,
            date_of_birth TIMESTAMP,
            age INT,
            registration_date TIMESTAMP,
            phone TEXT,
            cell TEXT,
            nationality TEXT,
            picture_url TEXT,
            username TEXT
        )
        """
        session.execute(table_query)
        logger.info(f"✓ Table '{CASSANDRA_TABLE}' is ready!")
        
        # Verify setup
        logger.info("Verifying Cassandra setup...")
        tables = session.execute(
            f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{CASSANDRA_KEYSPACE}'"
        )
        table_list = [row.table_name for row in tables]
        logger.info(f"Available tables in '{CASSANDRA_KEYSPACE}': {table_list}")
        
        # Close connection
        cluster.shutdown()
        logger.info("="*70)
        logger.info("✓ Cassandra setup completed successfully!")
        logger.info("="*70)
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Error setting up Cassandra: {e}")
        logger.exception("Stack trace:")
        return False

# ==================== SCHEMA DEFINITION ====================
user_schema = StructType([
    StructField("gender", StringType(), True),
    StructField("name", StructType([
        StructField("title", StringType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True)
    ]), True),
    StructField("location", StructType([
        StructField("street", StructType([
            StructField("number", IntegerType(), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("coordinates", StructType([
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)
        ]), True),
        StructField("timezone", StructType([
            StructField("offset", StringType(), True),
            StructField("description", StringType(), True)
        ]), True)
    ]), True),
    StructField("email", StringType(), True),
    StructField("login", StructType([
        StructField("uuid", StringType(), True),
        StructField("username", StringType(), True),
        StructField("password", StringType(), True),
        StructField("salt", StringType(), True),
        StructField("md5", StringType(), True),
        StructField("sha1", StringType(), True),
        StructField("sha256", StringType(), True)
    ]), True),
    StructField("dob", StructType([
        StructField("date", StringType(), True),
        StructField("age", IntegerType(), True)
    ]), True),
    StructField("registered", StructType([
        StructField("date", StringType(), True),
        StructField("age", IntegerType(), True)
    ]), True),
    StructField("phone", StringType(), True),
    StructField("cell", StringType(), True),
    StructField("id", StructType([
        StructField("name", StringType(), True),
        StructField("value", StringType(), True)
    ]), True),
    StructField("picture", StructType([
        StructField("large", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("thumbnail", StringType(), True)
    ]), True),
    StructField("nat", StringType(), True)
])

# ==================== SPARK SESSION ====================
def create_spark_session():
    """Create Spark session with Kafka and Cassandra connectors."""
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName("KafkaToCassandraStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", str(CASSANDRA_PORT)) \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    return spark

# ==================== PREPROCESSING ====================
def preprocess_user_data(df):
    """
    Apply comprehensive preprocessing to flatten and clean user data.
    
    Preprocessing Steps:
    1️⃣ Flatten Nested JSON Structure
    2️⃣ Generate or Normalize User ID (use login.uuid as primary key)
    3️⃣ Combine Address Fields
    4️⃣ Rename Fields to Match Schema
    5️⃣ Convert Date-Time Fields to Timestamp
    6️⃣ Handle Missing or Null Values
    7️⃣ Remove Sensitive Information (passwords, hashes)
    8️⃣ Validate Data Types
    9️⃣ Standardize Categorical Values (gender)
    """
    logger.info("Applying preprocessing transformations...")
    
    processed_df = df.select(
        # 1️⃣ & 2️⃣ Flatten and use UUID as primary key
        col("login.uuid").alias("user_id"),
        
        # 4️⃣ Rename fields to match Cassandra schema
        col("name.first").alias("first_name"),
        col("name.last").alias("last_name"),
        col("name.title").alias("title"),
        
        # 9️⃣ Standardize gender to lowercase
        lower(trim(col("gender"))).alias("gender"),
        
        col("email"),
        
        # 3️⃣ Combine address fields into single column
        concat_ws(", ",
            concat_ws(" ", 
                col("location.street.number").cast("string"),
                col("location.street.name")
            ),
            col("location.city"),
            col("location.state"),
            col("location.country")
        ).alias("address"),
        
        # Individual location fields
        col("location.city").alias("city"),
        col("location.state").alias("state"),
        col("location.country").alias("country"),
        col("location.postcode").cast("string").alias("postcode"),
        
        # 8️⃣ Validate and convert coordinate data types
        col("location.coordinates.latitude").cast("double").alias("latitude"),
        col("location.coordinates.longitude").cast("double").alias("longitude"),
        
        # 5️⃣ Convert date strings to timestamps
        to_timestamp(col("dob.date")).alias("date_of_birth"),
        col("dob.age").alias("age"),
        to_timestamp(col("registered.date")).alias("registration_date"),
        
        # Contact information
        col("phone"),
        col("cell"),
        
        # Nationality
        col("nat").alias("nationality"),
        
        # Picture URL (keeping medium size)
        col("picture.medium").alias("picture_url"),
        
        # 7️⃣ Remove sensitive information - only keep username
        col("login.username").alias("username")
    )
    
    # 6️⃣ Handle missing or null values
    # Filter out records without critical fields (user_id, email)
    processed_df = processed_df.filter(
        col("user_id").isNotNull() & 
        col("email").isNotNull()
    )
    
    # Fill missing values with defaults
    processed_df = processed_df \
        .fillna({
            "first_name": "Unknown",
            "last_name": "Unknown",
            "title": "Mr",
            "gender": "unspecified",
            "address": "Unknown",
            "city": "Unknown",
            "state": "Unknown",
            "country": "Unknown",
            "postcode": "00000",
            "phone": "N/A",
            "cell": "N/A",
            "nationality": "XX",
            "username": "unknown_user",
            "picture_url": ""
        }) \
        .fillna({
            "latitude": 0.0,
            "longitude": 0.0,
            "age": 0
        })
    
    logger.info("Preprocessing completed")
    return processed_df

# ==================== CASSANDRA WRITER ====================
def write_to_cassandra(batch_df, batch_id):
    """
    Write each micro-batch to Cassandra.
    
    Args:
        batch_df: DataFrame containing the micro-batch
        batch_id: Unique identifier for the batch
    """
    try:
        record_count = batch_df.count()
        
        if record_count > 0:
            logger.info(f"Processing batch {batch_id} with {record_count} records...")
            
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE) \
                .save()
            
            logger.info(f"✓ Batch {batch_id} written to Cassandra successfully!")
            
            # Log sample data
            logger.info(f"Sample from batch {batch_id}:")
            batch_df.select("user_id", "first_name", "last_name", "email", "country") \
                    .show(5, truncate=False)
        else:
            logger.info(f"Batch {batch_id} is empty, skipping...")
            
    except Exception as e:
        logger.error(f"✗ Error writing batch {batch_id} to Cassandra: {e}")
        raise

# ==================== MAIN FUNCTION ====================
def main():
    """
    Main function to run Spark Structured Streaming from Kafka to Cassandra.
    """
    try:
        logger.info("="*70)
        logger.info("Starting Kafka → Spark → Cassandra Streaming Pipeline")
        logger.info("="*70)
        
        # Step 1: Setup Cassandra (create keyspace and table)
        if not setup_cassandra():
            logger.error("✗ Failed to setup Cassandra. Exiting...")
            sys.exit(1)
        
        # Wait a moment for Cassandra to be ready
        logger.info("Waiting 2 seconds for Cassandra to stabilize...")
        time.sleep(2)
        
        # Step 2: Create Spark Session
        spark = create_spark_session()
        
        # Step 3: Read from Kafka
        logger.info(f"Connecting to Kafka broker: {KAFKA_BROKER}")
        logger.info(f"Subscribing to topic: {KAFKA_TOPIC}")
        
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "100") \
            .load()
        
        logger.info("Connected to Kafka successfully")
        
        # Step 4: Parse JSON from Kafka value
        logger.info("Parsing JSON messages from Kafka...")
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), user_schema).alias("data")
        ).select("data.*")
        
        # Step 5: Apply preprocessing
        processed_df = preprocess_user_data(parsed_df)
        
        # Step 6: Write to Cassandra using foreachBatch
        logger.info(f"Starting streaming query to Cassandra...")
        logger.info(f"Checkpoint location: {CHECKPOINT_DIR}")
        logger.info(f"Target: {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}")
        
        query = processed_df.writeStream \
            .foreachBatch(write_to_cassandra) \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        logger.info("="*70)
        logger.info("✓ Streaming query started successfully!")
        logger.info("Waiting for data from Kafka topic...")
        logger.info("Press Ctrl+C to stop the streaming job")
        logger.info("="*70)
        
        # Wait for termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("\n" + "="*70)
        logger.info("Streaming job interrupted by user (Ctrl+C)")
        logger.info("Shutting down gracefully...")
        logger.info("="*70)
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"\n✗ Fatal error in streaming job: {e}")
        logger.exception("Stack trace:")
        sys.exit(1)

if __name__ == "__main__":
    main()