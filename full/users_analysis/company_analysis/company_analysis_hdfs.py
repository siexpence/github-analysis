from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, to_json, regexp_replace, lower
from pyspark.sql.utils import AnalysisException
import time

def path_exists(spark, path):
    try:
        return len(spark.read.parquet(path).take(1)) > 0
    except AnalysisException:
        return False

def company_analysis():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisCompany").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        user_hdfs_path = "hdfs://master:9000/output/company_analysis"
        company_hdfs_path = "hdfs://master:9000/output/company_analysis_result"

        if not path_exists(spark, user_hdfs_path):
            continue

        user_data = spark.read.parquet(user_hdfs_path)

        user_data = user_data.withColumn("company", regexp_replace("company", "@", ""))
        user_data = user_data.withColumn("company", lower("company"))

        result_data = user_data.groupBy("company").agg(
            collect_list("login").alias("user_list"),
            count("login").alias("user_count")
        )

        result_data = result_data.withColumn("user_list", to_json("user_list"))

        result_data.write.mode("overwrite").parquet(company_hdfs_path)

        es_host = <your-es-ip>
        es_port = <your-es-port>
        es_index = "users_company_result"

        result_data.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index) \
                    .option("es.mapping.id", "company") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/company_analysis_result") \
                    .mode("overwrite") \
                    .save()

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

if __name__ == "__main__":
    company_analysis()
