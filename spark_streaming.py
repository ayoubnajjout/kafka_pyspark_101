from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, expr, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def create_spark_session():
    """Create a Spark session for streaming"""
    return (SparkSession.builder
            .appName("KafkaStreamProcessor")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
            .config("spark.sql.shuffle.partitions", "2")  # Reduce partitions for local testing
            .master("local[*]")
            .getOrCreate())

def define_schema():
    """Define the schema for the JSON data"""
    details_schema = StructType([
        StructField("product", StringType(), True),
        StructField("location", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    return StructType([
        StructField("user_id", IntegerType(), True),
        StructField("activity_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("details", details_schema, True)
    ])

def write_to_kafka(df, topic, output_mode="complete"):
    """Write DataFrame to a Kafka topic"""
    return (df
            .writeStream
            .outputMode(output_mode)
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", topic)
            .option("checkpointLocation", f"./checkpoints/{topic}")
            .start())

def main():
    """Main function to process Kafka stream with Spark"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce log verbosity
    
    # Define the schema for our JSON data
    json_schema = define_schema()
    
    # Create a streaming DataFrame from the Kafka source
    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "user_activities")
          .option("startingOffsets", "latest")
          .load())
    
    # Convert the Kafka message value from binary to string
    kafka_df = df.selectExpr("CAST(value AS STRING) as json_data")
    
    # Parse the JSON data with the defined schema
    parsed_df = kafka_df.select(
        from_json(col("json_data"), json_schema).alias("data")
    ).select("data.*")
    
    # Convert string timestamp to actual timestamp
    df_with_timestamp = parsed_df.withColumn(
        "event_timestamp", 
        expr("to_timestamp(timestamp)")
    )
    
    # PROCESSING #1: Count activities by type in 1-minute tumbling windows
    activity_counts = (df_with_timestamp
                      .groupBy(
                          window(col("event_timestamp"), "1 minute"),
                          col("activity_type")
                      )
                      .count()
                      .selectExpr(
                          "CAST(window.start AS STRING) as window_start",
                          "CAST(window.end AS STRING) as window_end",
                          "activity_type",
                          "count"
                      ))
    
    # Prepare the activity counts for Kafka output (convert to JSON)
    activity_kafka_output = activity_counts.select(
        to_json(struct("*")).alias("value")
    )
    
    # PROCESSING #2: Calculate average session duration by location
    location_duration = (df_with_timestamp
                        .groupBy("details.location")
                        .agg(avg("details.duration_seconds").alias("avg_duration"))
                        .orderBy("avg_duration", ascending=False))
    
    # Prepare the location duration for Kafka output
    location_kafka_output = location_duration.select(
        to_json(struct("*")).alias("value")
    )
    
    # PROCESSING #3: Calculate purchase value totals by user
    purchase_values = (df_with_timestamp
                      .filter(col("activity_type") == "purchase")
                      .filter(col("details.value").isNotNull())
                      .groupBy("user_id")
                      .agg(avg("details.value").alias("avg_purchase_value"))
                      .orderBy("avg_purchase_value", ascending=False))
    
    # Prepare the purchase values for Kafka output
    purchase_kafka_output = purchase_values.select(
        to_json(struct("*")).alias("value")
    )
    
    # Output the processed data to Kafka topics
    activity_query = write_to_kafka(activity_kafka_output, "activity_analytics")
    location_query = write_to_kafka(location_kafka_output, "location_analytics")
    purchase_query = write_to_kafka(purchase_kafka_output, "purchase_analytics")
    
    # Also output to console for monitoring (optional)
    console_query = (activity_counts
                    .writeStream
                    .outputMode("complete")
                    .format("console")
                    .option("truncate", False)
                    .start())
    
    # Wait for all queries to terminate
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    # Create checkpoint directory
    import os
    os.makedirs("./checkpoints", exist_ok=True)
    os.makedirs("./checkpoints/activity_analytics", exist_ok=True)
    os.makedirs("./checkpoints/location_analytics", exist_ok=True)
    os.makedirs("./checkpoints/purchase_analytics", exist_ok=True)
    
    main()