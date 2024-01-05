from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, to_json, split
from pyspark.sql.utils import AnalysisException
import time

def location_analysis():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisLocation").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        user_hdfs_path = "hdfs://master:9000/output/location_analysis"
        location_hdfs_path = "hdfs://master:9000/output/location_analysis_result"

        user_data = spark.read.parquet(user_hdfs_path)

        user_data = user_data.withColumn("location", split(col("location"), ",")[0])

        result_data = user_data.groupBy("location").agg(
            collect_list("login").alias("user_list"),
            count("login").alias("user_count")
        )

        result_data = result_data.withColumn("user_list", to_json("user_list"))

        result_data.write.mode("overwrite").parquet(location_hdfs_path)

        es_host = <your-es-ip>
        es_port = <your-es-port>
        es_index = "users_location_result"

        result_data.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index) \
                    .option("es.mapping.id", "location") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/location_analysis_result") \
                    .mode("overwrite") \
                    .save()

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

if __name__ == "__main__":
    location_analysis()
