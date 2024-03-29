from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, MapType
from pyspark.sql.functions import explode, sum, desc     

# Schema定义
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

    # 请替换掉你的kafka ip和port
    kafka_server = 'your kafka'
    topic_name = 'github_repos'
  
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", topic_name) \
        .load()

    user_data = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    user_data = user_data.dropDuplicates(['id'])

    # 定义fork和star的权重比例
    fork_weight = 0.6
    star_weight = 0.4
    issues_weight = 0.4
    commits_weight = 0.6

    # 计算加权得分并进行排
    repos_with_scores = user_data.withColumn("popular", col("forks_count") * fork_weight + col("stars_count") * star_weight) \
        .withColumn("active", col("open_issues_count") * issues_weight + col("commits_count") * commits_weight)
    repos_sorted = repos_with_scores.orderBy(col("popular").desc()).limit(1000)

    # 将languages中的map拆分成多行
    exploded_languages = repos_sorted.select("id", explode(col("languages")).alias("language", "byte_count"))

    # 对每种语言的字节数量进行汇总并按降序排列

    sorted_languages = exploded_languages.groupBy("language") \
    .agg(sum("byte_count").alias("total_lines_count")) \
    .orderBy(col("total_lines_count").desc()).limit(100)
    sorted_languages.show(100, truncate=False)

       # 请替换掉你的elastic research ip和端口
    es_host = "your_es_host"  # 替换为你的Elasticsearch主机地址
    es_port = "your_es_port"  # 替换为你的Elasticsearch端口号
    es_index = "toprepos_sorted_languages"  # 替换为你希望的Elasticsearch索引名

    # 写入Elasticsearch
    sorted_languages.write.format("org.elasticsearch.spark.sql") \
        .option("es.nodes", es_host) \
        .option("es.port", es_port) \
        .option("es.resource", es_index) \
        .mode("overwrite") \
        .save()

    spark.stop()


if __name__ == "__main__":
    main()
