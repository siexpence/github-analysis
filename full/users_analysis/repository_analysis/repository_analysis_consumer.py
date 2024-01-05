import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

json_schema = StructType([
    StructField("login", StringType()),
    StructField("id", IntegerType()),
    StructField("node_id", StringType()),
    StructField("avatar_url", StringType()),
    StructField("gravatar_id", StringType()),
    StructField("url", StringType()),
    StructField("html_url", StringType()),
    StructField("followers_url", StringType()),
    StructField("following_url", StringType()),
    StructField("gists_url", StringType()),
    StructField("starred_url", StringType()),
    StructField("subscriptions_url", StringType()),
    StructField("organizations_url", StringType()),
    StructField("repos_url", StringType()),
    StructField("events_url", StringType()),
    StructField("received_events_url", StringType()),
    StructField("type", StringType()),
    StructField("site_admin", BooleanType()),
    StructField("name", StringType()),
    StructField("company", StringType()),
    StructField("blog", StringType()),
    StructField("location", StringType()),
    StructField("email", StringType()),
    StructField("hireable", BooleanType()),
    StructField("bio", StringType()),
    StructField("twitter_username", StringType()),
    StructField("public_repos", IntegerType()),
    StructField("public_gists", IntegerType()),
    StructField("followers", IntegerType()),
    StructField("following", IntegerType()),
    StructField("created_at", TimestampType()), 
    StructField("updated_at", TimestampType())
])

access_token = <your-token>

def get_user_repositories(repos_url):
    headers = {'Authorization': f'token {access_token}'}

    response = requests.get(repos_url, headers=headers)

    if response.status_code == 200:
        repositories = response.json()
        repositories_list = [repositorie['name'] for repositorie in repositories]
        return repositories_list
    else:
        print(f"Error: Unable to fetch repositories for {repos_url}.")
        return []

def get_repository_stars(owner, repo):
    endpoint = f'https://api.github.com/repos/{owner}/{repo}'
    headers = {'Authorization': f'token {access_token}'}

    response = requests.get(endpoint, headers=headers)
    
    if response.status_code == 200:
        repo_info = response.json()
        return repo_info
    else:
        print(f"Error: Unable to fetch stars for the repository {owner}/{repo}.")
        return {}
    
def user_repository_analysis(owner, repos_url):
    repositories = get_user_repositories(repos_url)
    stargazers_count = {}
    for repo in repositories:
        repo_info = get_repository_stars(owner, repo)
        repo_name = repo_info.get("name")
        repo_star = repo_info.get("stargazers_count")
        if repo_star:
            stargazers_count[repo_name] = repo_star
    
    if len(stargazers_count) == 0:
        return "NULL"
    
    best_repository = max(stargazers_count, key=lambda x: stargazers_count[x])
    print(owner, best_repository, stargazers_count[best_repository])
    return best_repository

def repository_analysis():
    spark = SparkSession.builder.appName("GitHubAnalysisRepository").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").config("spark.kafka.consumerThread", "true").getOrCreate()

    kafka_server = <your-kafka-server-ip>
    topic_name = 'github-users-0'
    kafka_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("startingOffsets", "earliest") \
        .option("subscribe", topic_name) \
        .load()

    user_data = kafka_stream.selectExpr("CAST(value AS STRING)").select(from_json("value", json_schema).alias("user_info")).select("user_info.*")
    
    repository_analysis = user_data.select("login", "repos_url")

    repository_analysis_udf = udf(user_repository_analysis, StringType())

    repository_analysis = repository_analysis.withColumn("repository", repository_analysis_udf(col("login"), col("repos_url")))
    
    query_console = repository_analysis.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .start()

    hdfs_path = "hdfs://master:9000/output/repository_analysis"
    ck_path = "hdfs://master:9000/checkpoint/repository_analysis"

    query_hdfs = repository_analysis.writeStream \
                        .outputMode("append") \
                        .format("parquet") \
                        .option("path", hdfs_path) \
                        .option("checkpointLocation", ck_path) \
                        .start()
    
    es_host = <your-es-ip>
    es_port = <your-es-port>
    es_index = "users_repository"

    query_es = repository_analysis.writeStream \
                    .outputMode("append") \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.resource", es_index) \
                    .option("es.nodes", es_host) \
                    .option("es.port", es_port) \
                    .option("es.mapping.id", "login") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/repository_analysis") \
                    .start()
    
    query_console.awaitTermination()
    query_hdfs.awaitTermination()
    query_es.awaitTermination()
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    repository_analysis()
