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

def get_repository_languages(owner, repo):
    endpoint = f'https://api.github.com/repos/{owner}/{repo}/languages'
    headers = {'Authorization': f'token {access_token}'}

    response = requests.get(endpoint, headers=headers)
    
    if response.status_code == 200:
        languages = response.json()
        return languages
    else:
        print(f"Error: Unable to fetch languages for the repository {owner}/{repo}.")
        return {}
    
def user_language_analysis(owner, repos_url):
    repositories = get_user_repositories(repos_url)
    languages_count = {}
    for repo in repositories:
        repo_languages = get_repository_languages(owner, repo)

        for language, bytes_of_code in repo_languages.items():
            languages_count[language] = languages_count.get(language, 0) + bytes_of_code
    
    if len(languages_count) == 0:
        return "NULL"
    
    favorite_language = max(languages_count, key=lambda x: languages_count[x])
    print(owner, favorite_language)
    return favorite_language

def language_analysis():
    spark = SparkSession.builder.appName("GitHubGetLanguage").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").config("spark.kafka.consumerThread", "true").getOrCreate()

    kafka_server = <your-kafka-server-ip>
    topic_name = 'github-users-0'
    kafka_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("startingOffsets", "earliest") \
        .option("subscribe", topic_name) \
        .load()

    user_data = kafka_stream.selectExpr("CAST(value AS STRING)").select(from_json("value", json_schema).alias("user_info")).select("user_info.*")
    
    language_analysis = user_data.select("login", "repos_url")

    language_analysis_udf = udf(user_language_analysis, StringType())

    language_analysis = language_analysis.withColumn("language", language_analysis_udf(col("login"), col("repos_url")))
    
    query_console = language_analysis.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .start()

    hdfs_path = "hdfs://master:9000/output/language_analysis"
    ck_path = "hdfs://master:9000/checkpoint/language_analysis"

    query_hdfs = language_analysis.writeStream \
                        .outputMode("append") \
                        .format("parquet") \
                        .option("path", hdfs_path) \
                        .option("checkpointLocation", ck_path) \
                        .start()
    
    es_host = <your-es-ip>
    es_port = <your-es-port>
    es_index = "users_language"

    query_es = language_analysis.writeStream \
                    .outputMode("append") \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.resource", es_index) \
                    .option("es.nodes", es_host) \
                    .option("es.port", es_port) \
                    .option("es.mapping.id", "login") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/language_analysis") \
                    .start()
    
    query_console.awaitTermination()
    query_hdfs.awaitTermination()
    query_es.awaitTermination()
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    language_analysis()
