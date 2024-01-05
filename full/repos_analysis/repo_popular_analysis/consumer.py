from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, MapType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True),
    StructField("stars_count", IntegerType(), True),
    StructField("forks_count", IntegerType(), True),
    StructField("commits_count", IntegerType(), True),
    StructField("open_issues_count", IntegerType(), True),
    StructField("languages", MapType(StringType(), IntegerType()), True)
])

def main():
    spark = SparkSession.builder \
        .appName("KafkaReposConsumer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_server = '100.97.119.60:9092'
    topic_name = 'github_repos'
    hdfs_path = 'hdfs://master:9000/output/all_repos'

    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", topic_name) \
        .load()

    user_data = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Write data to HDFS in batch mode
    user_data.write \
        .mode("append") \
        .format("parquet") \
        .save(hdfs_path)


    # 定义fork和star的权重比例
    fork_weight = 0.6
    star_weight = 0.4

    # 计算加权得分并进行排序
    repos_sorted = user_data.dropDuplicates(['id']).withColumn("weighted_score", col("forks_count") * fork_weight + col("stars_count") * star_weight)\
        .orderBy(col("weighted_score").desc())


    # query.awaitTermination()
    repos_sorted.show(50,truncate=False)
    
    # 输出前1000个repo到Elasticsearch
    es_host = "100.97.119.60"  # 替换为你的Elasticsearch主机地址
    es_port = "9200"  # 替换为你的Elasticsearch端口号
    es_index = "top_repos"  #
    # 写入Elasticsearch
    repos_sorted.limit(1000).write.format("org.elasticsearch.spark.sql") \
        .option("es.nodes", es_host) \
        .option("es.port", es_port) \
        .option("es.resource", es_index) \
        .mode("overwrite") \
        .save()
    
    spark.stop()


if __name__ == "__main__":
    main()
