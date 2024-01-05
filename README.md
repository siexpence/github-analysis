# 《大数据分析技术与实践》：基于大数据的GitHub数据分析系统

## 项目简介
本项目是由《大数据分析技术与实践》课程第二小组完成的基于大数据的GitHub数据分析系统，旨在分析GitHub上的海量开源项目和开发活动，以揭示热点技术话题和热门开发者社区的趋势。系统通过分析编程语言或框架的使用趋势、软件的演变历程、开发者活跃度等，提供了对项目质量、软件维护成本等的预测。

## 小组分工
- **用户API**：洪蔚杰
- **项目API**：李锦凯
- **事件API**：宋晓童
- **提交API**：于倬浩
- **总体架构**：于倬浩

## 文件说明

本项目包含两个主要文件夹：`validation/`和`full/`，分别用于验证分析结果和展示完整的项目代码。

### `validation/`
我们提供一个专为验证分析结果设计的环境，以方便老师快速核实我们的工作成果，无需重新爬取和处理数据。

- **用于验证的Docker环境**：`docker-compose.yml`部署包括Elasticsearch和Kibana在内的服务。这些服务已预装和配置好，可以直接运行。

- **数据集成**：Elasticsearch中已预加载了我们处理好的数据，这意味着用户可以立即开始验证和探索分析结果，而无需额外的数据导入步骤。

- **简化验证**：通过这个环境，用户可以直接访问Kibana来查看和分析数据，这个过程不需要编写或运行任何额外的代码。这样做的目的是为了提供一个快速、方便的方式来核实我们项目的有效性和准确性。

### `full/`
这个文件夹包含了我们项目的完整代码和所需的所有环境设置，可以完整地复现我们的工作流程，从数据爬取到分析，以及最终的数据可视化过程。

- **完整的Docker环境**：`docker-compose.yml`文件设置了一个完整的数据分析环境，包括zookeeper, kafka, spark, hadoop, elasticsearch, kibana等服务。

- **数据分析处理代码**：此环境包括用于数据爬取和数据处理的代码，这些代码涵盖了从数据获取到处理的整个流程。


## 验证分析结果

为了方便快速地验证我们的分析结果，我们提供了预打包的Elasticsearch和Kibana镜像。您可以按照以下步骤进行验证：

1. **下载镜像文件**：
   请访问以下URL下载Elasticsearch和Kibana的tar文件：
   https://disk.pku.edu.cn:443/link/B1B3D0EE881650865CD809E2D3032EE7 有效期限：2024-02-02 23:59

2. **加载镜像**：
   使用Docker加载下载的镜像文件。
   ```shell
   docker load --input elasticsearch-submit.tar
   docker load --input kibana-submit.tar
   ```

3. **运行Docker Compose**：
   在包含`docker-compose.yml`的文件夹中，运行以下命令以启动Elasticsearch和Kibana服务。
   ```shell
   cd validation/
   docker-compose up
   ```

4. **访问Kibana**：
   在浏览器中输入`http://localhost:5601`访问Kibana。在这里，您可以直接查看和分析预先加载的数据，验证我们的分析结果，其中端口号在docker-compose.yml定义。

请确保您的系统中已安装Docker，并且对Docker具有一定的使用经验。

## 运行完整项目

如果您希望运行并探索我们项目的完整代码，可以遵循以下步骤：

1. **准备环境**：
   确保您的计算机上安装了Docker和Docker Compose。这是运行项目所必需的。

2. **下载项目代码**：
   克隆或下载我们的项目代码到您的本地机器。

3. **安装依赖**：
   根据`full/`文件夹中的README指导安装必要的依赖包。

4. **运行Docker Compose**：
   在包含完整`docker-compose.yml`文件的目录下运行：
   ```shell
   docker-compose up
   ```
   这将根据配置文件启动所有必需的服务，包括zookeeper, kafka, spark, hadoop, elasticsearch, kibana等。

5. **访问和探索数据**：
   数据处理完成后，您可以访问Kibana（通常位于`localhost:5601`）来探索和分析数据。

请按照`full/`文件夹中的额外README进行详细操作，以确保正确配置和使用整个系统。
