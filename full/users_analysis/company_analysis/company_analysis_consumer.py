from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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

def company_analysis():
    spark = SparkSession.builder.appName("GitHubGetCompany").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

    kafka_server = <your-kafka-server-ip>
    topic_name = 'github-users-0'
    kafka_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("startingOffsets", "earliest") \
        .option("subscribe", topic_name) \
        .load()

    user_data = kafka_stream.selectExpr("CAST(value AS STRING)").select(from_json("value", json_schema).alias("user_info")).select("user_info.*")

    company_analysis = user_data.select("login", "company")
    
    company_analysis = company_analysis.filter(col("company").isNotNull())
    
    query_console = company_analysis.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .start()

    hdfs_path = "hdfs://master:9000/output/company_analysis"
    ck_path = "hdfs://master:9000/checkpoint/company_analysis"

    query_hdfs = company_analysis.writeStream \
                        .outputMode("append") \
                        .format("parquet") \
                        .option("path", hdfs_path) \
                        .option("checkpointLocation", ck_path) \
                        .start()
    
    es_host = <your-es-ip>
    es_port = <your-es-port>
    es_index = "users_company"

    query_es = company_analysis.writeStream \
                    .outputMode("append") \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.resource", es_index) \
                    .option("es.nodes", es_host) \
                    .option("es.port", es_port) \
                    .option("es.mapping.id", "company") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/company_analysis") \
                    .start()
    
    query_console.awaitTermination()
    query_hdfs.awaitTermination()
    query_es.awaitTermination()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    company_analysis()
