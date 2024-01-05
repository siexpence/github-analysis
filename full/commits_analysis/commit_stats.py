from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StringType, StructType, StructField, TimestampType

# Kafka and Elasticsearch configuration
kafka_bootstrap_servers = "<your-host>:<your-kafka-port>"
kafka_topic = "list_commits_v1"
hdfs_output_path = (
    "hdfs://master:9000/output/test_commit_consumer"  # Replace with your HDFS path
)
es_host = "<your-host>"  # Replace with your Elasticsearch host
es_port = "<your-es-port>"  # Replace with your Elasticsearch port
es_index = "github_commits"  # Replace with your desired Elasticsearch index name
# Schema for Kafka data
schema = StructType(
    [
        StructField("repo_owner", StringType()),
        StructField("repo_name", StringType()),
        StructField("sha", StringType()),
        StructField(
            "commit",
            StructType(
                [
                    StructField(
                        "author",
                        StructType(
                            [
                                StructField("name", StringType()),
                                StructField("email", StringType()),
                                StructField("date", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "commiter",
                        StructType(
                            [
                                StructField("name", StringType()),
                                StructField("email", StringType()),
                                StructField("date", StringType()),
                            ]
                        ),
                    ),
                    StructField("message", StringType()),
                ]
            ),
        ),
        StructField(
            "author",
            StructType(
                [
                    StructField("login", StringType()),
                    StructField("id", StringType()),
                    StructField("node_id", StringType()),
                    StructField("html_url", StringType()),
                ]
            ),
        ),
        StructField(
            "committer",
            StructType(
                [
                    StructField("login", StringType()),
                    StructField("id", StringType()),
                    StructField("node_id", StringType()),
                    StructField("html_url", StringType()),
                ]
            ),
        ),
    ]
)


def main():
    spark = SparkSession.builder.appName("KafkaCommitConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    commits_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    commits_df = commits_df.withColumn("commit_date", col("commit.author.date"))

    query_console = (
        commits_df.writeStream.outputMode("append").format("console").start()
    )

    query_hdfs = (
        commits_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", hdfs_output_path)
        .option("checkpointLocation", "hdfs://master:9000/checkpoint/hdfs")
        .start()
    )

    query_es = (
        commits_df.writeStream.outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", "commits_test_v1")
        .option("es.nodes", es_host)
        .option("es.port", es_port)
        .option("es.mapping.id", "sha")
        .option("es.write.operation", "upsert")
        .option("es.index.auto.create", "true")
        .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic")
        .start()
    )
    # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./share/test.py
    query_console.awaitTermination()
    query_hdfs.awaitTermination()
    query_es.awaitTermination()


if __name__ == "__main__":
    main()
