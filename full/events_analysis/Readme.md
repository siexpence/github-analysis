## 事件API代码及运行说明

### 1. 文件介绍

- **events_producer**：从github上面爬取关于事件的数据（一段时间内所有的事件，特定仓库和用户相关的事件），并将数据传送到kafka
- **events_type_analysis**：事件类型分析
- **events_activity_analysis**：基于事件的仓库/用户活跃度分析
- **events_status_analysis**：基于事件的特定仓库/用户状态分析
- **events_trend_prediction**：事件时间趋势分析
- **project_similarity_analysis**：基于事件的项目相似度分析
- **project_quality_evaluation**：基于事件的项目质量评估



### 2. 运行步骤

**a. 安装程序运行依赖的包**

```shell
pip3 install -r requirements.txt
```

**b. 替换程序中的占位符**

```
<your-token>
<your-kafka-server-ip>
<your-kafka-server-port>
<your-es-ip>
<your-es-port>
```

**c. 爬取事件数据，并将数据传到kafka**

```shell
cd events_producer

#! 获取一段时间内所有的事件数据
nohup python3 events_producer.py --all_events 1 --sleep_seconds 3600 --starting_date 2021-01-01T0:00:00 > ./output/producer_op_1.log 2>&1 &

#! 获取特定仓库/用户的事件数据
nohup python3 events_producer.py --all_events 0 --repo_list_path "./repo_list.json" --sleep_seconds 3600 --starting_date 2021-01-01T0:00:00 > producer_op_1.log 2>&1 &
```

**d. 从kafka将获取数据并进行实时/离线数据处理**

- **离线数据处理**：文件夹下有文件名后缀为consumer的python脚本（如project_similarity_analysis_consumer.py），用于从kafka获取数据并存储到hdfs中；还有一个后缀名为hdfs的脚本（如project_similarity_analysis_hdfs.py）用于从hdfs中获取数据，并利用spark中提供的工具对数据进行分析处理，并将处理结果存在hdfs并传送到elasticsearch进行可视化。以**基于事件的项目相似度分析**文件夹为例运行：

```shell
#! 实时数据处理实例：
cd project_similarity_analysis

#! consumer脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./project_similarity_analysis_consumer.py > consumer_op.log 2>&1 &

#! hdfs脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./project_similarity_analysis_hdfs.py > hdfs_op.log 2>&1 &
```


- **实时数据处理**：文件夹下会有一个文件名后缀为consumer的python脚本，只需运行该脚本就可从kafka获取相应的数据，进行简单的处理后输出到elasticsearch进行可视化。以**事件时间趋势分析**为例运行：

```shell
#! 实时数据处理：
cd events_trend_prediction

#! consumer脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./events_trend_prediction_consumer.py > consumer_op.log 2>&1 &
```





