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

# 基于event分析 仓库 活跃度
def repo_activity_analysis():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisIssues").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        data_hdfs_path = "hdfs://master:9000/output/event_activation"
        result_hdfs_path = "hdfs://master:9000/output/event_activation_result"

        if not path_exists(spark, data_hdfs_path):
            print("Path does not exist")
            continue

        df = spark.read.parquet(data_hdfs_path)

        df = df.select('id', 'type', 'created_at', df.repo.id.alias('repo_id'))
        
        # 种类，活跃天数，总数的加权
        # 然后按照'repo_id'进行分组，并收集其他列的信息
        df_grouped = df.groupBy("repo_id") \
                    .agg(F.collect_list("type").alias("type_list"),
                            F.count("repo_id").alias("repo_count"),
                            F.collect_list("created_at").alias("created_at_list")) 

        # 将日期转化成仓库活跃天数
        # 使用transform函数对created_at_list中的每个元素进行日期转换
        df_grouped = df_grouped.withColumn("created_at_dates", 
                                F.expr("transform(created_at_list, x -> to_date(x, 'yyyy-MM-dd HH:mm:ss'))"))

        # 创建新的列active_days，通过对转换后的日期列表执行array_distinct和size函数来计算活跃天数
        df_grouped = df_grouped.withColumn('active_days', F.size(F.array_distinct("created_at_dates")))

        result_data = df.withColumn("type_num", F.size(F.col('type_list')))
        
        result_data.write.mode("overwrite").parquet(result_hdfs_path)

        es_host = "<your-ip-here>"
        es_port = "<your-port-here>"
        es_index = "repo_activity_result"

        # 将处理好的数据输出到es进行可视化
        result_data.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index) \
                    .option("es.mapping.id", "id") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/event_repo_activation") \
                    .mode("overwrite") \
                    .save()

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

# 基于event分析 用户 活跃度
def user_activity_analysis():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisIssues").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        data_hdfs_path = "hdfs://master:9000/output/event_activation"
        result_hdfs_path = "hdfs://master:9000/output/event_activation_result"

        df = spark.read.parquet(data_hdfs_path)

        # 取出事件id，类型，发生时间等关键信息
        df = df.select('id', 'type', 'created_at', df.actor.id.alias('actor_id'))
        
        # 种类，活跃天数，事件总数
        # 然后按照'actor_id'进行分组，并收集其他列的信息
        df_grouped = df.groupBy("actor_id") \
                    .agg(F.collect_list("type").alias("type_list"),
                            F.count("actor_id").alias("actor_count"),
                            F.collect_list("created_at").alias("created_at_list")) 


        # 将日期转化成用户活跃天数
        df_grouped = df_grouped.withColumn("created_at_dates", 
                                F.expr("transform(created_at_list, x -> to_date(x, 'yyyy-MM-dd HH:mm:ss'))"))

        # 创建新的列active_days，通过对转换后的日期列表执行array_distinct和size函数来计算活跃天数
        df_grouped = df_grouped.withColumn('active_days', F.size(F.array_distinct("created_at_dates")))

        result_data = df.withColumn("type_num", F.size(F.col('type_list')))

        result_data.write.mode("overwrite").parquet(result_hdfs_path)

        es_host = "<your-es-ip>"
        es_port = "<your-es-port>"
        es_index = "user_activity_result"

        # 将处理好的数据输出到es进行可视化
        result_data.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index) \
                    .option("es.mapping.id", "id") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/event_user_activation") \
                    .mode("overwrite") \
                    .save()

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

if __name__ == "__main__":
    repo_activity_analysis()
    user_activity_analysis()
