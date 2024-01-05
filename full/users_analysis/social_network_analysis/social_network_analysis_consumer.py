import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, ArrayType
import networkx as nx
import matplotlib.pyplot as plt
import time

social_network = nx.Graph()
last_time = None

def draw_network():
    pos = nx.kamada_kawai_layout(social_network)
    plt.figure(figsize=(16, 12))
    nx.draw(social_network, pos, with_labels=True, node_size=3200, node_color="skyblue", font_size=20, font_color="black", font_weight="bold", edge_color="gray", linewidths=1, alpha=0.7)

    plt.savefig("user_network.png")

    plt.title("User Network")
    plt.show()

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

def get_user_followers(login):
    endpoint = f'https://api.github.com/users/{login}/followers'
    headers = {'Authorization': f'token {access_token}'}

    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        followers = response.json()
        followers_list = [follower['login'] for follower in followers]
        return followers_list
    else:
        print(f"Error: Unable to fetch followers for {login}.")
        return []

def get_user_following(login):
    endpoint = f'https://api.github.com/users/{login}/following'
    headers = {'Authorization': f'token {access_token}'}

    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        following = response.json()
        following_list = [followed['login'] for followed in following]
        return following_list
    else:
        print(f"Error: Unable to fetch following for {login}.")
        return []

def mutual_followers_analysis(login):
    followers = get_user_followers(login)
    following = get_user_following(login)

    mutual_followers = list(set(followers) & set(following))

    print(login)
    print(mutual_followers)

    social_network.add_nodes_from(login)
    social_network.add_nodes_from(mutual_followers)
    edges = [(login, follower) for follower in mutual_followers]
    social_network.add_edges_from(edges)

    return mutual_followers

def social_network_analysis():
    spark = SparkSession.builder.appName("GitHubAnalysisSocialNetwork").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").config("spark.kafka.consumerThread", "true").getOrCreate()

    kafka_server = <your-kafka-server-ip>
    topic_name = 'github-users-0'
    kafka_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("startingOffsets", "earliest") \
        .option("subscribe", topic_name) \
        .load()

    user_data = kafka_stream.selectExpr("CAST(value AS STRING)").select(from_json("value", json_schema).alias("user_info")).select("user_info.*")

    social_network_analysis = user_data.select("login")
    mutual_followers_udf = udf(mutual_followers_analysis, ArrayType(StringType()))

    social_network_analysis = social_network_analysis.withColumn("mutual_followers", mutual_followers_udf(col("login")))
    
    cur_time = time.time()
    if last_time is None or cur_time - last_time >= 3600:
        print("Drawing user network...")
        draw_network()
        last_time = cur_time

    query_console = social_network_analysis.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .start()

    hdfs_path = "hdfs://master:9000/output/social_network_analysis"
    ck_path = "hdfs://master:9000/checkpoint/social_network_analysis"

    query_hdfs = social_network_analysis.writeStream \
                        .outputMode("append") \
                        .format("parquet") \
                        .option("path", hdfs_path) \
                        .option("checkpointLocation", ck_path) \
                        .start()
    
    es_host = <your-es-ip>
    es_port = <your-es-port>
    es_index = "users_social_network"

    query_es = social_network_analysis.writeStream \
                    .outputMode("append") \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.resource", es_index) \
                    .option("es.nodes", es_host) \
                    .option("es.port", es_port) \
                    .option("es.mapping.id", "login") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/social_network_analysis") \
                    .start()
    
    query_console.awaitTermination()
    query_hdfs.awaitTermination()
    query_es.awaitTermination()
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    social_network_analysis()

