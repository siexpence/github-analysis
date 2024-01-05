from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, MapType
from pyspark.sql.functions import explode, sum, desc     
from pyspark.sql.functions import array, concat
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql.functions import corr

# 加载数据的部分

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

spark = SparkSession.builder \
    .appName("KafkaReposConsumer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 请替换掉你的kafka ip和port
kafka_server = 'your kafka'
topic_name = 'github_repos'
hdfs_path = 'hdfs://master:9000/output/all_repos'

kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topic_name) \
    .load()

user_data = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
user_data = user_data.dropDuplicates(['id'])




# 计算四项指标之间的相关性
correlation_matrix = user_data.select(corr("open_issues_count", "stars_count").alias("open_issues_stars"),
                                     corr("open_issues_count", "forks_count").alias("open_issues_forks"),
                                     corr("open_issues_count", "commits_count").alias("open_issues_commits"),
                                     corr("stars_count", "forks_count").alias("stars_forks"),
                                     corr("stars_count", "commits_count").alias("stars_commits"),
                                     corr("forks_count", "commits_count").alias("forks_commits"))

# 找出相关性最强的指标对
correlation_matrix.show()
strongest_correlation = max([(col, value) for col, value in correlation_matrix.first().asDict().items()],
                            key=lambda x: abs(x[1]))

print(strongest_correlation)
