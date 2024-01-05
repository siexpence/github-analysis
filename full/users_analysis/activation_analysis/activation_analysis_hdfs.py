from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.utils import AnalysisException
import time

schema = StructType([
    StructField("login", StringType(), True),
    StructField("public_repos", IntegerType()),
    StructField("public_gists", IntegerType()),
    StructField("activation", IntegerType(), True),
])

def path_exists(spark, path):
    try:
        return len(spark.read.parquet(path).take(1)) > 0
    except AnalysisException:
        return False

def activation_analysis():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisActivation").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        user_hdfs_path = "hdfs://master:9000/output/activation_analysis"
        activation_hdfs_path = "hdfs://master:9000/output/activation_analysis_result"
        local_path = "./result.json"

        if not path_exists(spark, user_hdfs_path):
            continue

        user_data = spark.read.parquet(user_hdfs_path)

        if path_exists(spark, activation_hdfs_path):
            result_data = spark.read.parquet(activation_hdfs_path)
        else:
            result_data = user_data

        min_activation = result_data.agg({"activation": "min"}).collect()[0][0]

        for user_row in user_data.rdd.collect():
            login = user_row["login"]
            activation = user_row["activation"]

            if login not in result_data.select("login").rdd.flatMap(lambda x: x).collect() and activation > min_activation:
                new_user_data = [(user_row["login"], int(user_row["public_repos"]), int(user_row["public_gists"]), int(user_row["activation"]))]
                new_user_df = spark.createDataFrame(new_user_data, schema)
                result_data = result_data.union(new_user_df)

        result_data = result_data.dropDuplicates(["login"])

        result_data = result_data.orderBy(col("activation").desc()).limit(100)

        result_data.write.mode("overwrite").parquet(activation_hdfs_path)
        # result_data.write.mode("overwrite").json(local_path)

        es_host = <your-es-ip>
        es_port = <your-es-port>
        es_index = "users_activation_result"

        result_data.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index) \
                    .option("es.mapping.id", "login") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/activation_analysis_result") \
                    .mode("overwrite") \
                    .save()

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

if __name__ == "__main__":
    activation_analysis()
