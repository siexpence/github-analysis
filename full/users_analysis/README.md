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

1. **安装程序运行依赖的包**

```shell
pip3 install -r requirements.txt
```

2. **替换程序中的占位符**

```
<your-token>
<your-kafka-server-ip>
<your-es-ip>
<your-es-port>
```

3. **爬取用户数据，并将数据传到kafka**

```shell
cd crawl_users

#! 获取用户数据
python3 crawl_users.py
```

4. **离线数据处理**

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

5. **实时数据处理**：

- 文件名后缀为consumer的python程序，只需运行该脚本就可从kafka获取相应的数据，进行处理后输出到elasticsearch进行可视化。

- 以**用户编程语言分析**文件夹为例运行：

```shell
#! 实时数据处理实例：
cd language_analysis

#! consumer脚本运行命令
nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.spark:spark-sql_2.12:3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 ./language_analysis_consumer.py >/dev/null 2>&1 &
```

6. **数据可视化**

- 访问docker-compose中配置的Kibana地址，即可对处理后的数据进行可视化分析。


