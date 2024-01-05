# 完整项目配置运行说明

## 构建镜像

```shell
cd full/spark_image/

#! 下载Hadoop，在这里我们使用Hadoop 3.2.4
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.4/hadoop-3.2.4.tar.gz

#! 构建镜像
docker build -t spark-333-hadoop-324 .
```

## 启动容器

请您替换`docker-compose.yml`中的占位符`<your-ip-here>`为您宿主机的地址（需从容器内可访问到），然后运行以下命令启动容器，并进入spark master容器：

```shell
cd full/

#! 启动容器
docker compose up -d

#! 启动Hadoop
docker exec -it <your-spark-master-container-id> /opt/start-hadoop.sh

#! 进入spark master容器
docker exec -it <your-spark-master-container-id> bash
```

接下来的所有操作都在spark master容器中进行，注意，由于GitHub在国内的连接性问题，其API访问稳定性无法保证，如需运行数据爬取部分代码，请配置代理。

## 准备工作

1. **安装依赖包**

```shell
#! 切换到容器和宿主机共享的目录
cd share/
pip3 install -r ./commits_analysis/requirements.txt
pip3 install -r ./events_analysis/requirements.txt
pip3 install -r ./users_analysis/requirements.txt
pip3 install -r ./repos_analysis/requirements.txt
```

2. **替换程序中的占位符**

```
<your-token> 您的GitHub访问密钥
<your-host> 您的主机地址，需从容器内可访问
<your-es-ip> 您的Elasticsearch IP地址
<your-es-port> 您的Elasticsearch端口
<your-kafka-server-ip> 您的Kafka Broker IP地址
<your-kafka-port> 您的Kafka Broker端口
<your-llm-host> 您部署大型语言模型的主机地址，需从容器内可访问
<your-llm-port> 您部署大型语言模型的端口
```



## 用户API代码及运行说明

### 1. 文件介绍

- `crawl_users.py` 爬取用户数据
- `activation_analysis` 用户活跃程度统计
- `company_analysis` 企业人才分布统计
- `create_analysis` 用户创建时间统计和预测
- `focus_users_analysis` 焦点用户分析
- `language_analysis` 用户编程语言分析
- `location_analysis` 用户地区统计
- `repository_analysis` 用户项目分析
- `social_network_analysis` 社交网络分析

### 2. 运行步骤

1. **获取用户数据**

```shell
cd crawl_users

#! 获取用户数据
python3 crawl_users.py
```

2. **离线数据处理**

- 文件名后缀为consumer的python程序（如activation_analysis_consumer.py），用于从kafka获取数据并存储到hdfs中。

- 文件名后缀为hdfs的python程序（如activation_analysis_hdfs.py）用于从hdfs中获取数据，并利用spark中提供的工具对数据进行分析处理，并将处理结果存在hdfs并传送到elasticsearch进行可视化。

- 以**用户活跃程度统计**文件夹为例运行：

```shell
#! 离线数据处理实例：
cd activation_analysis

#! consumer脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./activation_analysis_consumer.py >/dev/null 2>&1 &

#! hdfs脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./activation_analysis_hdfs.py >/dev/null 2>&1 &
```

3. **实时数据处理**：

- 文件名后缀为consumer的python程序，只需运行该脚本就可从kafka获取相应的数据，进行处理后输出到elasticsearch进行可视化。

- 以**用户编程语言分析**文件夹为例运行：


```shell
#! 实时数据处理实例：
cd language_analysis

#! consumer脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./language_analysis_consumer.py >/dev/null 2>&1 &
```

4. **数据可视化**

- 访问docker-compose中配置的Kibana地址，即可对处理后的数据进行可视化分析。



## 仓库API代码及运行说明

### 1. 文件介绍

- `producer.py` 爬取仓库数据
- `repos_popular_analysis` 基于仓库的各项指标，对受欢迎程度、活跃程度进行分析
- `language_analysis` 基于仓库使用编程语言长度的统计，对编程语言受欢迎程度的分析
- `cor_analysis` 基于爬取的仓库数据，对仓库中各项指标进行相关性分析

### 2. 运行步骤

1. **获取仓库数据**

```shell
#! 切换到repos分析目录下
cd repos_analysis

#! 运行python脚本，启动producer，获取repos
python3 events_producer.py 
```

2. **进行仓库受欢迎程度和活跃程度分析**

- **批式处理**：在项目数据分析中以批式处理数据的方式为主，以下分析（包括c、d部分）都采用批处理的方式进行

```shell
#! 切换到目录下：
cd repos_analysis/repos_popular_analysis

#! 读取kafka中的repo数据，进行受欢迎程度、活跃程度排序后，将发送数据到elasticsearch
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./send_es.py
```

3. **进行编程语言受欢迎程度分析**

```shell
#! 切换到目录下：
cd repos_analysis/repos_popular_analysis

#! 读取kafka中的repo数据，对编程语言进行统计分析后排序，并将发送数据到elasticsearch
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./consumer.py

#! 读取kafka中的repo数据，先排序中top100 repo，然后对其编程语言进行统计排序后，将发送数据到elasticsearch
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./top_repos_lan.py
```

4. **进行编程语言受欢迎程度分析**

```shell
#! 切换到目录下：
cd repos_analysis/cor_analysis

#! 读取kafka中的repo数据，对其fork, stat, open issue, commit 数量，进行相关性分析，将结果输出到控制台
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./fork_star.py

#! 说明：由于获取repo数据较少，相关性分析的结果没有呈现出预期效果，各指标之间相关性较弱
```

5. **数据可视化**

- 访问docker-compose中配置的Kibana地址，即可对处理后的数据进行可视化分析。



## 事件API代码及运行说明

### 1. 文件介绍

- `events_producer` 爬取事件数据（一段时间内所有的事件，特定仓库和用户相关的事件）
- `events_type_analysis` 事件类型分析
- `events_activity_analysis` 基于事件的仓库/用户活跃度分析
- `events_status_analysis` 基于事件的特定仓库/用户状态分析
- `events_trend_prediction` 事件时间趋势分析
- `project_similarity_analysis` 基于事件的项目相似度分析
- `project_quality_evaluation` 基于事件的项目质量评估

### 2. 运行步骤

1. **获取事件数据**

```shell
cd events_producer

#! 获取一段时间内所有的事件数据
nohup python3 events_producer.py --all_events 1 --sleep_seconds 3600 --starting_date 2021-01-01T0:00:00 > ./output/producer_op_1.log 2>&1 &

#! 获取特定仓库/用户的事件数据
nohup python3 events_producer.py --all_events 0 --repo_list_path "./repo_list.json" --sleep_seconds 3600 --starting_date 2021-01-01T0:00:00 > producer_op_1.log 2>&1 &
```

2. **离线数据处理**

- 文件夹下有文件名后缀为consumer的python脚本（如project_similarity_analysis_consumer.py），用于从kafka获取数据并存储到hdfs中；还有一个后缀名为hdfs的脚本（如project_similarity_analysis_hdfs.py）用于从hdfs中获取数据，并利用spark中提供的工具对数据进行分析处理，并将处理结果存在hdfs并传送到elasticsearch进行可视化。以**基于事件的项目相似度分析**文件夹为例运行：

```shell
#! 实时数据处理实例：
cd project_similarity_analysis

#! consumer脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./project_similarity_analysis_consumer.py > consumer_op.log 2>&1 &

#! hdfs脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./project_similarity_analysis_hdfs.py > hdfs_op.log 2>&1 &
```

3. **实时数据处理**

- 文件夹下会有一个文件名后缀为consumer的python脚本，只需运行该脚本就可从kafka获取相应的数据，进行简单的处理后输出到elasticsearch进行可视化。以**事件时间趋势分析**为例运行：

```shell
#! 实时数据处理：
cd events_trend_prediction

#! consumer脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./events_trend_prediction_consumer.py > consumer_op.log 2>&1 &
```

4. **数据可视化**

- 访问docker-compose中配置的Kibana地址，即可对处理后的数据进行可视化分析。



## 提交API代码及运行说明

### 文件介绍

+ `list_commits.py` 爬取提交数据
+ `list_issues.py` 爬取Issue数据
+ `list_prs.py` 爬取Pull Request数据
+ `commit_stats.py` 对提交数据进行统计分析
+ `commit_cls.py` 基于大模型的提交分析
+ `pulls_cls.py` 基于大模型的Pull Request贡献分析

### 运行步骤

1. **获取提交数据**

```shell
nohup python3 list_commits.py repo_list.json 3600 2023-01-01T00:00:00Z >/dev/null 2>&1 &
nohup python3 list_prs.py repo_list.json 3600 2023-01-01T00:00:00Z >/dev/null 2>&1 &
nohup python3 list_issues.py repo_list.json 3600 2023-01-01T00:00:00Z >/dev/null 2>&1 &
```

2. **部署大型语言模型**

- 考虑到大模型占用空间较大，我们提交的镜像中不包含模型权重，需从https://huggingface.co/mistralai/Mistral-7B-Instruct-v0.1 下载权重到本地路径，并使用如下指令部署模型，请修改您需使用的GPU ID、端口以及模型的本地路径。在实验中，我们使用单张NVIDIA RTX 4090 24GB GPU完成推理。

```shell
docker run --rm -d --gpus "device=<your-gpu-id>" --shm-size 32g -p <your-llm-port>:80 -v <path-to-your-model>:/data --name tgi ghcr.io/huggingface/text-generation-inference:1.1.0 --model-id /data --sharded false --max-input-length=3000 --max-total-tokens=4096 --max-best-of=8 --max-stop-sequences=20 --max-batch-prefill-tokens=4096 --trust-remote-code
```

3. **数据处理**

```shell
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./commits_cls.py >/dev/null 2>&1 &

nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./commits_stats.py >/dev/null 2>&1 &

nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./pulls_cls.py >/dev/null 2>&1 &
```

4. **数据可视化**

- 访问docker-compose中配置的Kibana地址，即可对处理后的数据进行可视化分析。
