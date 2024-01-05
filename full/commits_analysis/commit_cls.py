from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StringType, StructType, StructField, TimestampType
from pyspark.sql.functions import from_json, col, to_timestamp, udf
from text_generation import Client
from random import choice

# Kafka and Elasticsearch configuration
host = "<your-host>"
kafka_bootstrap_servers = f"{host}:<your-kafka-port>"
kafka_topic = "list_commits_cls_test"
hdfs_output_path = "hdfs://master:9000/output/commit_cls"  # Replace with your HDFS path
es_host = f"{host}"  # Replace with your Elasticsearch host
es_port = "9200"  # Replace with your Elasticsearch port
es_index = "commits_cls_v2"  # Replace with your desired Elasticsearch index name
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

cache = {}  # Global cache for storing results


def do_cls(sha, repo_owner, repo_name, commit_message, date):
    if sha in cache:
        return cache[sha]
    try:
        client_urls = [
            "http://<your-llm-host>:<your-llm-port>",
        ]
        client = Client(base_url=choice(client_urls), timeout=15)
        tmpl = """<s>[INST] You are an expert software engineer and you are classifying commits from GitHub according to their commit messages to ['bugfix', 'feature', 'format', 'refactor', 'docs'], only output from these classes without saying anything else. repository: {repo}, commit message: {msg} [/INST]"""
        commit_message = commit_message.split("\n")[0][:512]
        # print(repo_owner, repo_name, commit_message[:512])
        tp = tmpl.format(repo=f"{repo_owner}/{repo_name}", msg=commit_message)
        res = client.generate(tp, max_new_tokens=10).generated_text
        print(repo_owner, repo_name, len(commit_message), res, date)
        allowed_types = ["bugfix", "feature", "format", "refactor", "docs"]
        cls_res = [t for t in allowed_types if t in res]
        if len(cls_res) == 0:
            cache[sha] = "unknown"
            return cache[sha]
        else:
            cache[sha] = cls_res[0]
            return cache[sha]
    except Exception as e:
        print(f"Error in do_cls: {e}")
        cache[sha] = "unknown"
        return cache[sha]


# Register the UDF
classify_commit_udf = udf(do_cls, StringType())


def main():
    spark = SparkSession.builder.appName("KafkaCommitConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    commits_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    commits_df = commits_df.withColumn("commit_date", col("commit.author.date"))

    commits_df = commits_df.withColumn(
        "commit_type",
        classify_commit_udf(
            commits_df["sha"],
            commits_df["repo_owner"],
            commits_df["repo_name"],
            commits_df["commit.message"],
            commits_df["commit.author.date"],
        ),
    )

    query_console = (
        commits_df.writeStream.outputMode("append").format("console").start()
    )

    query_hdfs = (
        commits_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", hdfs_output_path)
        .option("checkpointLocation", "hdfs://master:9000/checkpoint/hdfs/commits_cls")
        .start()
    )

    query_es = (
        commits_df.writeStream.outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", es_index)
        .option("es.nodes", es_host)
        .option("es.port", es_port)
        .option("es.mapping.id", "sha")
        .option("es.write.operation", "upsert")
        .option("es.index.auto.create", "true")
        .option(
            "checkpointLocation", "hdfs://master:9000/checkpoint/elastic/commits_cls"
        )
        .start()
    )
    # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./share/test.py
    query_console.awaitTermination()
    query_hdfs.awaitTermination()
    query_es.awaitTermination()


if __name__ == "__main__":
    main()
