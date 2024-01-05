from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import ArrayType, LongType, StructType, StructField, StringType, NullType, BooleanType, TimestampType

# 分析实时的事件数量变化，流式
# Kafka and Elasticsearch configuration
kafka_bootstrap_servers = "<your-kafka-server-ip>:<your-kafka-server-port>,<your-kafka-server-ip>:<your-kafka-server-port>"
kafka_topic = "events_list_all" 
hdfs_output_path = "hdfs://master:9000/output/event_trend"  # HDFS path
ck_path = "hdfs://master:9000/checkpoint/event_trend"
es_host = "<your-es-ip>"
es_port = "<your-es-port>"
es_index = "event_trend"  # Elasticsearch index name

# Schema for Kafka data
schema_all_events = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("display_login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]), True),
    StructField("repo", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True)
    ]), True),
    StructField("payload", StructType([
        StructField("repository_id", LongType(), True),
        StructField("push_id", LongType(), True),
        StructField("size", LongType(), True),
        StructField("distinct_size", LongType(), True),
        StructField("ref", StringType(), True),
        StructField("head", StringType(), True),
        StructField("before", StringType(), True),
        StructField("commits", ArrayType(StructType([
            StructField("sha", StringType(), True),
            StructField("author", StructType([
                StructField("email", StringType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("message", StringType(), True),
            StructField("distinct", BooleanType(), True),
            StructField("url", StringType(), True)
        ])), True)
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True)
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
    events_df = df.select(from_json(col("value").cast("string"), schema_all_events).alias("data")).select("data.*")
    events_df = events_df.withColumn("event_date", col("created_at"))  # 将日期单独作为一列加进去
    
    # 保存到hdfs,用于长期的事件趋势变化分析
    query_hdfs = events_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", hdfs_output_path) \
        .option("checkpointLocation", ck_path) \
        .start()

    # 将实时数据输出到es
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
        .option("checkpointLocation", ck_path) \
        .start()

    query_hdfs.awaitTermination()
    query_es.awaitTermination()

if __name__ == "__main__":
    main()

