from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, to_json
from pyspark.sql.utils import AnalysisException
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
import time

def quality_evaluation():
    df = df.select('id', 'type', 'created_at', df.repo.id.alias('repo_id'))
        
    # 种类，活跃天数，总数的加权
    # 按照'repo_id'进行分组，并收集每个仓库的事件数量、类型数量以及活跃天数等信息
    df_grouped = df.groupBy("repo_id") \
                .agg(F.collect_list("type").alias("type_list"),  # 类型列表
                        F.count("repo_id").alias("repo_count"), # 总的事件数量
                        F.collect_list("created_at").alias("created_at_list")) 
    # 将日期转化成仓库活跃天数
    # 使用transform函数对created_at_list中的每个元素进行日期转换
    df_grouped = df_grouped.withColumn("created_at_dates", 
                            F.expr("transform(created_at_list, x -> to_date(x, 'yyyy-MM-dd HH:mm:ss'))"))

    # 创建新的列active_days，通过对转换后的日期列表执行array_distinct和size函数来计算活跃天数
    df_grouped = df_grouped.withColumn('active_days', F.size(F.array_distinct("created_at_dates")))

    df = df.withColumn("type_num", F.size(F.col('type_list')))

    # 根据3个重要参数对仓库的质量进行评估
    # 具体的，可以根据重要程度对仓库进行加权，得到一个评估分
    # 权重值应根据实际需求进行调整
    repo_count_weight = 0.3  
    type_num_weight = 0.5
    active_days_weight = 0.2

    df_grouped = df_grouped.withColumn(
        'score',
        repo_count_weight * F.col('repo_count') + weight2 * F.col('type_num_weight') + weight3 * F.col('active_days_weight'))
    
    return df_grouped

def path_exists(spark, path):
    try:
        return len(spark.read.parquet(path).take(1)) > 0
    except AnalysisException:
        return False

def events_quality_analysis():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisIssues").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        data_hdfs_path = "hdfs://master:9000/output/events_quality_analysis"
        result_hdfs_path = "hdfs://master:9000/output/events_quality_analysis_result"


        events_data = spark.read.parquet(data_hdfs_path)

        if not path_exists(spark, data_hdfs_path):
            continue

        if path_exists(spark, result_hdfs_path):
            result_data = spark.read.parquet(result_hdfs_path)
        else:
            result_data = events_data


        # 对某一具体项目的质量进行评估
        result_data = quality_evaluation(result_data)

        result_data.write.mode("overwrite").parquet(data_hdfs_path)

        es_host = "<your-es-ip>"
        es_port = "<your-es-port>"
        es_index = "repo_status_result"

        result_data.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index) \
                    .option("es.mapping.id", "id") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/events_status_analysis") \
                    .mode("overwrite") \
                    .save()

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

if __name__ == "__main__":
    events_quality_analysis()
