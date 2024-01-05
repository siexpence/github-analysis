from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql.functions import from_json, col, to_timestamp, udf
from text_generation import Client
from random import choice

# Kafka and Elasticsearch configuration
host = "<your-host>"

kafka_bootstrap_servers = f"{host}:<your-port>"

hdfs_output_path = "hdfs://master:9000/output/pulls_cls"  # Replace with your HDFS path
es_host = f"{host}"  # Replace with your Elasticsearch host
es_port = "<your-es-port>"  # Replace with your Elasticsearch port
es_index = "list_pulls_v4"  # Replace with your desired Elasticsearch index name
# Schema for Kafka data

# Schema for GitHub Pull Request data
pull_request_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("repo_name", StringType()),
        StructField("repo_owner", StringType()),
        StructField("author", StringType()),
        StructField("author_association", StringType()),
        StructField("state", StringType()),
        StructField("title", StringType()),
        StructField("body", StringType()),
        StructField("from_branch", StringType()),
        StructField("to_branch", StringType()),
        StructField("created_at", StringType()),  # Dates as strings
        StructField("updated_at", StringType()),
        StructField("closed_at", StringType()),
        StructField("merged_at", StringType()),
        StructField("draft", StringType()),
        StructField("finish_time", FloatType()),  # Time as float
        StructField("assignee", StringType()),
    ]
)

cache = {}  # Global cache for storing results


def do_cls(
    id,
    repo_owner,
    repo_name,
    pull_author_association,
    pull_title,
    pull_body,
    pull_state,
    time_taken,
    date,
):
    if id in cache:
        return cache[id]
    try:
        client_urls = [
            "http://<your-llm-host>:<your-llm-port>",
        ]
        client = Client(base_url=choice(client_urls), timeout=15)
        tmpl = """<s>[INST] You are an expert software engineer and you are classifying a pull request on GitHub according to impact, into ONE of ['none', 'minor', 'major'] class, indicating the potential effect or changes the pull request can be, give a very brief summary of the PR in 10 words, followed by a newline, then output from one of these classes at last, this is important. 'none' means chores, fixing formats, typos, docs or other changes that does not affect code outcomes. \n 'minor' means bug-fixes, minor new features, components or improvements, minor change in logic. \n'major' means very significant structural change, very important new features, major bug fixes. Think carefully and be objective, especially when giving 'major' flags, do not overestimate the impact, do not use 'major' very often, this is very important. \n\nRepository: {repo_owner}/{repo_name}\nAuthor association: {pull_author_association}\nTitle: {pull_title}\nPR State: {pull_state}\nTime taken: {time_taken}\nPull request body: {body} [/INST]"""

        pull_body = str(pull_body)
        pull_body = pull_body.strip()[:300]
        time_taken = f"{time_taken:.1f} hours" if time_taken >= 0 else "not finished"

        tp = tmpl.format(
            repo=f"{repo_owner}/{repo_name}",
            repo_owner=repo_owner,
            repo_name=repo_name,
            pull_author_association=pull_author_association,
            pull_title=pull_title,
            pull_state=pull_state,
            time_taken=time_taken,
            body=pull_body,
        )

        res = client.generate(tp, max_new_tokens=50).generated_text
        allowed_types = ["none", "minor", "major"]
        try:
            search_txt = res.strip().split("\n")[-1].lower()
            cls_res = [t for t in allowed_types if t in search_txt]
            if len(cls_res) > 0:
                parsed_res = cls_res[0]
            else:
                parsed_res = "unknown"
        except:
            parsed_res = "unknown"
        print(
            f"{date}\t{parsed_res}\t{repo_owner}/{repo_name}\t\t{len(tp)}\t{pull_title}"
        )
        cache[id] = parsed_res
        return cache[id]
    except Exception as e:
        print(f"Error in do_cls: {e}")
        cache[id] = "unknown"
        return cache[id]


# Register the UDF
classify_pull_udf = udf(do_cls, StringType())


def main():
    spark = SparkSession.builder.appName("KafkaPullsConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "list_pulls_v3")
        .option("startingOffsets", "latest")
        .load()
    )

    pulls_df = df.select(
        from_json(col("value").cast("string"), pull_request_schema).alias("data")
    ).select("data.*")
    # def do_cls(id, repo_owner, repo_name, pull_author_association, pull_title, pull_body, pull_state, time_taken):
    pulls_df = pulls_df.withColumn("create_date", col("created_at"))

    pulls_df = pulls_df.withColumn(
        "pull_type",
        classify_pull_udf(
            pulls_df["id"],
            pulls_df["repo_owner"],
            pulls_df["repo_name"],
            pulls_df["author_association"],
            pulls_df["title"],
            pulls_df["body"],
            pulls_df["state"],
            pulls_df["finish_time"],
            pulls_df["created_at"],
        ),
    )

    query_console = pulls_df.writeStream.outputMode("append").format("console").start()

    query_hdfs = (
        pulls_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", hdfs_output_path)
        .option("checkpointLocation", "hdfs://master:9000/checkpoint/hdfs/pulls_cls")
        .start()
    )

    query_es = (
        pulls_df.writeStream.outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", es_index)
        .option("es.nodes", es_host)
        .option("es.port", es_port)
        .option("es.mapping.id", "id")
        .option("es.write.operation", "upsert")
        .option("es.index.auto.create", "true")
        .option(
            "checkpointLocation", "hdfs://master:9000/checkpoint/elastic/pulls_cls_v5"
        )
        .option("asyncProgressTrackingEnabled", "true")
        .start()
    )
    # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./share/test.py
    # query_console.awaitTermination()
    # query_hdfs.awaitTermination()
    query_es.awaitTermination()


if __name__ == "__main__":
    main()
