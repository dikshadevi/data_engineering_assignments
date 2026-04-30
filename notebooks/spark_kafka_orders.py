from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder \
    .appName("KafkaOrdersStreaming") \
    .master("spark://spark:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("total_amount", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("city", StringType(), True),
    StructField("created_at", StringType(), True)
])


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()


orders_df = kafka_df.select(
    col("key").cast("string").alias("message_key"),
    col("value").cast("string").alias("message_value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)


parsed_orders_df = orders_df.select(
    "message_key",
    "topic",
    "partition",
    "offset",
    "timestamp",
    from_json(col("message_value"), order_schema).alias("order")
).select(
    "message_key",
    "topic",
    "partition",
    "offset",
    "timestamp",
    col("order.order_id"),
    col("order.customer_name"),
    col("order.product"),
    col("order.quantity"),
    col("order.price"),
    col("order.total_amount"),
    col("order.status"),
    col("order.payment_mode"),
    col("order.city"),
    col("order.created_at")
)


query = parsed_orders_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()