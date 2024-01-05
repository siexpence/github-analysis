from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, to_json
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import udf, col
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import VectorAssembler, Normalizer, StringIndexer, OneHotEncoder
from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import PCA as PCAml
from pyspark.ml import Pipeline
import networkx as nx
import time

def kmeans_cluster(df):
    df = df.select('id', 'type', 'created_at', df.repo.id.alias('repo_id'))

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

    df = df.withColumn("type_num", F.size(F.col('type_list')))

    # VectorAssembler用于将repo_count, active_days以及type_vec组合成一个特征向量
    assembler = VectorAssembler(inputCols=["repo_count", "active_days", "type_num"],
                                outputCol="features")

    # KMeans模型设定
    kmeans = KMeans(k=3, featuresCol="features")

    # 设立一个 Pipeline 来处理以上所有步骤
    pipeline = Pipeline(stages=[assembler, kmeans])


    # 将DataFrame传入此Pipeline
    model = pipeline.fit(df)

    # 执行转换，并获得聚类结果
    transformed = model.transform(df)

    # 将 PySpark DataFrame 转为 Pandas DataFrame
    pandas_df = transformed.toPandas()

    # 生成以 repo_id 和 prediction_clustering 这两列为边，权重是频率的 networkx 图
    G = nx.from_pandas_edgelist(pandas_df, "repo_id", "prediction")

    nx.draw(G, with_labels=True)
    # 展示图形
    plt.savefig('simi_graph.png')
    plt.show()

    result_data = result_data.withColumn("user_list", to_json("user_list"))


# 计算项目特征之间的余弦相似度
def cos_similarity(df):
    # 创建特征向量
    new_df = df.select(df['id'].alias('id'), df['actor.id'].alias('actor_id'), df['repo.id'].alias('repo_id'))

    assembler = VectorAssembler(inputCols=["actor_id", "repo_id"], outputCol="features")
    df_features = assembler.transform(new_df)

    # 特征标准化
    normalizer = Normalizer(inputCol="features", outputCol="norm_features")
    df_normalized = normalizer.transform(df_features)

    # 计算余弦相似度的 UDF
    cosine_similarity = udf(
        lambda x, y: float(x.dot(y)) / (Vectors.norm(x, 2) * Vectors.norm(y, 2)) if Vectors.norm(x, 2) and Vectors.norm(y, 2) else 0.0,
        FloatType()
    )

    # 计算仓库之间的相似度
    df_with_similarity = df_normalized.alias("df1").crossJoin(df_normalized.alias("df2")) \
                                    .select(
                                        col("df1.id").alias("name1"),
                                        col("df2.id").alias("name2"),
                                        cosine_similarity("df1.norm_features", "df2.norm_features").alias("similarity")  # 返回值
                                    ) \
                                    .filter(col("name1") != col("name2")).distinct()

    # 选择相似度高于阈值的记录以减少可视化的复杂性
    similarity_threshold = 0.5
    similar_edges = df_with_similarity.filter(df_with_similarity.similarity > similarity_threshold)\
                                    .select('name1', 'name2').rdd \
                                    .map(lambda row: (row['name1'], row['name2']))\
                                    .collect()

    # 使用所有简化的边创建无向图
    G = nx.Graph()
    for edge in similar_edges:
        G.add_edge(edge[0], edge[1], weight=edge[2])

    # 使用 networkx 和 matplotlib 可视化图
    pos = nx.spring_layout(G, k=0.15, iterations=20)
    plt.figure(figsize=(12, 12))
    nx.draw(G, pos, with_labels=True, node_size=700, node_color='lightblue', linewidths=0.25, font_size=10, font_weight='bold', edge_color='grey')
    edge_labels = dict(((u, v), f"{d['weight']:.2f}") for u, v, d in G.edges(data=True))
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, label_pos=0.5, font_size=7)
    plt.title('GitHub Repositories Similarity Graph')
    plt.axis('off')
    plt.savefig('simi_graph.png')  # 保存项目相似度图

def path_exists(spark, path):
    try:
        return len(spark.read.parquet(path).take(1)) > 0
    except AnalysisException:
        return False

# 项目相似度计算
def events_similarity_analysis():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisIssue").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()
        # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0")

        data_hdfs_path = "hdfs://master:9000/output/events_similarity_analysis"
        result_hdfs_path = "hdfs://master:9000/output/event_similarity_result"

        if not path_exists(spark, data_hdfs_path):
            print("Path does not exist")
            continue
        
        df = spark.read.parquet(data_hdfs_path)
        
        if path_exists(spark, result_hdfs_path):
            result_data = spark.read.parquet(result_hdfs_path)
        else:
            result_data = df

        kmeans_cluster(result_data)  # 获得项目相似度图

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

if __name__ == "__main__":
    events_similarity_analysis()

