from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, to_json
from pyspark.sql.utils import AnalysisException
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
import time


def path_exists(spark, path):
    try:
        return len(spark.read.parquet(path).take(1)) > 0
    except AnalysisException:
        return False

# 基于 event type 分析github平台的特征
def event_type_analysis():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisIssues").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        data_hdfs_path = "hdfs://master:9000/output/event_type"
        result_hdfs_path = "hdfs://master:9000/output/event_type_result"

        if not path_exists(spark, data_hdfs_path):
            print("Path does not exist")
            continue

        df = spark.read.parquet(data_hdfs_path)

        # 取出事件id，类型，发生时间等关键信息
        df = df.select('id', 'type', 'created_at')
        
        # 种类，活跃天数，事件总数
        # 然后按照'actor_id'进行分组，并收集其他列的信息
        result_data = df.groupBy("type") \
                    .agg(F.count("type").alias("type_count")) 

        result_data.write.mode("overwrite").parquet(result_hdfs_path)

        es_host = "<your-es-ip>"
        es_port = "<your-es-port>"
        es_index = "event_type_result"

        # 将处理好的数据输出到es进行可视化
        result_data.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index) \
                    .option("es.mapping.id", "id") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/event_type_analysis") \
                    .mode("overwrite") \
                    .save()

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

if __name__ == "__main__":
    event_type_analysis()
