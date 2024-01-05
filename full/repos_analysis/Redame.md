## 项目API代码及运行说明

### 1. 文件介绍

- **repos_analysis**：关于本次实验项目仓库进行爬取、分析的全部代码

  - **producer.py**：在github rest API中爬取仓库repo相关的（实验中爬取了3年内的部分repos），并将数据发送到kafka

  - **repos_popular_analysis**：基于仓库的各项指标，对受欢迎程度、活跃程度进行分析

  - **language_analysis**：基于仓库使用编程语言长度的统计，对编程语言受欢迎程度的分析

  - **cor_analysis**：基于爬取的仓库数据，对仓库中各项指标进行相关性分析

    

### 2. 运行步骤

**a. 爬取事件数据，并将数据发送到kafka**

```shell
#! 切换到repos分析目录下
cd repos_analysis

#! 运行python脚本，启动producer，获取repos
#! 注意，运行前，需要替换脚本中的github access token 和 kafka的相关ip、port
python3 events_producer.py 
```

**b. 进行仓库受欢迎程度和活跃程度分析**

- **批式处理**：在项目数据分析中以批式处理数据的方式为主，以下分析（包括c、d部分）都采用批处理的方式进行

```shell
#! 切换到目录下：
cd repos_analysis/repos_popular_analysis


#! 读取kafka中的repo数据，进行受欢迎程度、活跃程度排序后，将发送数据到elastic research
#! 注意，运行前，需要替换脚本中的kafka和elastic research相关的ip、port 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./send_es.py
```



**c. 进行编程语言受欢迎程度分析**

```shell
#! 切换到目录下：
cd repos_analysis/repos_popular_analysis

#! 读取kafka中的repo数据，对编程语言进行统计分析后排序，并将发送数据到elastic research
#! 注意，运行前，需要替换脚本中的kafka和elastic research相关的ip、port 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./consumer.py

#! 读取kafka中的repo数据，先排序中top100 repo，然后对其编程语言进行统计排序后，将发送数据到
   elastic research
#! 注意，运行前，需要替换脚本中的kafka和elastic research相关的ip、port 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./top_repos_lan.py
```



**d. 进行编程语言受欢迎程度分析**

```shell
#! 切换到目录下：
cd repos_analysis/cor_analysis

#! 读取kafka中的repo数据，对其fork, stat, open issue, commit 数量，进行相关性分析，将结果输出到控制台
#! 注意，运行前，需要替换脚本中的kafka相关的ip、port 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./fork_star.py

#! 说明：由于获取repo数据较少，相关性分析的结果没有呈现出预期效果，各指标之间相关性较弱

```
