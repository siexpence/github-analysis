from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.utils import AnalysisException
import time

schema = StructType([
    StructField("login", StringType(), True),
    StructField("followers", IntegerType()),
    StructField("following", IntegerType()),
])

def path_exists(spark, path):
    try:
        return len(spark.read.parquet(path).take(1)) > 0
    except AnalysisException:
        return False

def focus_users_analysis():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisFocusUsers").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        user_hdfs_path = "hdfs://master:9000/output/focus_users_analysis"
        focus_users_hdfs_path = "hdfs://master:9000/output/focus_users_analysis_result"

        if not path_exists(spark, user_hdfs_path):
            continue

        user_data = spark.read.parquet(user_hdfs_path)

        if path_exists(spark, focus_users_hdfs_path):
            result_data = spark.read.parquet(focus_users_hdfs_path)
        else:
            result_data = user_data

        min_followers = result_data.agg({"followers": "min"}).collect()[0][0]

        for user_row in user_data.rdd.collect():
            login = user_row["login"]
            followers = user_row["followers"]

            if login not in result_data.select("login").rdd.flatMap(lambda x: x).collect() and followers > min_followers:
                new_user_data = [(user_row["login"], int(user_row["followers"]), int(user_row["following"]))]
                new_user_df = spark.createDataFrame(new_user_data, schema)
                result_data = result_data.union(new_user_df)

        result_data = result_data.dropDuplicates(["login"])

        result_data = result_data.orderBy(col("followers").desc()).limit(100)

        result_data.write.mode("overwrite").parquet(focus_users_hdfs_path)

        es_host = <your-es-ip>
        es_port = <your-es-port>
        es_index = "users_focus_result"

        result_data.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index) \
                    .option("es.mapping.id", "login") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/focus_users_analysis_result") \
                    .mode("overwrite") \
                    .save()

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

if __name__ == "__main__":
    focus_users_analysis()
