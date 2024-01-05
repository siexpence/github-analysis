from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import ArrayType, LongType, StructType, StructField, StringType, NullType, BooleanType, TimestampType

# Kafka and Elasticsearch configuration
kafka_bootstrap_servers = "<your-kafka-server-ip>:<your-kafka-server-port>,<your-kafka-server-ip>:<your-kafka-server-port>"
kafka_topic = "repo_events_list"  
hdfs_output_path = "hdfs://master:9000/output/event_status"  # HDFS path
ck_path = "hdfs://master:9000/checkpoint/event_status"  
es_host = "<your-es-ip>"
es_port = "<your-es-port>"
es_index = "event_status_time"  # Elasticsearch index name

# Schema for Kafka data
schema_events = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", 
                StructType([
                    StructField("id", LongType(), True),
                    StructField("login", StringType(), True),
                    StructField("display_login", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("avatar_url", StringType(), True)
                ]), True),
    StructField("repo", 
                StructType([
                    StructField("id", LongType(), True),
                    StructField("name", StringType(), True),
                    StructField("url", StringType(), True)
                ]), True),
    StructField("payload", 
                StructType([
                    StructField("action", StringType(), True)
                ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("org",
                StructType([
                    StructField("id", LongType(), True),
                    StructField("login", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("avatar_url", StringType(), True)
                ]), True)
])

def main():
    spark = SparkSession.builder \
        .appName("KafkaEventConsumer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # 按照 schema 进行解析和转换
    events_df = df.select(from_json(col("value").cast("string"), schema_events).alias("data")).select("data.*")
    events_df = events_df.withColumn("event_date", col("created_at"))  # 将日期单独作为一列加进去


    # 输出到hdfs
    query_hdfs = events_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", hdfs_output_path) \
        .option("checkpointLocation", ck_path) \
        .start()

    # 输出到es, 进行实时状态分析
    query_es = events_df \
        .writeStream \
        .outputMode("append") \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", es_index) \
        .option("es.nodes", es_host) \
        .option("es.port", es_port) \
        .option("es.mapping.id", "id") \
        .option("es.write.operation", "upsert") \
        .option("es.index.auto.create", "true") \
        .option("checkpointLocation", "hdfs://master:9000/checkpoint/event_status") \
        .start()

    query_hdfs.awaitTermination()
    query_es.awaitTermination()

if __name__ == "__main__":
    main()

