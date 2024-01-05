import requests
import json
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# 请替换掉你的github access_token
access_token = 'your github token'
headers = {'Authorization': f'token {access_token}'}


def get_repository_info(repo):
    repo_full_name = repo['full_name']
    repo_name = repo['name']
    url = repo['url']
    stargazers_count = repo['stargazers_count']
    forks_count = repo['forks_count']
    commits_url = f"https://api.github.com/repos/{repo_full_name}/commits"
    commits_response = requests.get(commits_url, headers=headers)
    if commits_response.status_code == 200:
        commits = commits_response.json()
        commits_count = len(commits)
    else:
        commits_count = 0
    # comm= repo['subscribers_count']
    open_issues_count = repo['open_issues_count']
    repo_languages_url = repo['languages_url']
    languages_response = requests.get(repo_languages_url, headers=headers)
    if languages_response.status_code == 200:
        repo_languages = languages_response.json()
    else:
        repo_languages = None

    repo_info = {
        'id': repo['id'],
        'name': repo_name,
        'url': url,
        'stars_count': stargazers_count,
        'forks_count': forks_count,
        'commits_count': commits_count,
        'open_issues_count': open_issues_count,
        'languages': repo_languages
    }

    return repo_info


def get_repositories(page_number,x):
    # 计算一年前的日期
    one_year_ago = datetime.now() - timedelta(days=3*365-x*60)
    created_date = one_year_ago.strftime('%Y-%m-%d')

    # GitHub API查询参数
    params = {
        'q': f'created:>{created_date}',
        'sort': 'stars',
        'order': 'desc',
        'per_page': 100,  # 每页返回的结果数量（最大100）
        'page': page_number  # 页码
    }

    # GitHub API终端点URL
    url = 'https://api.github.com/search/repositories'

    # 发送GET请求并获取存储库信息
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch repositories. Status code: {response.status_code}")
        return None


def produce_to_kafka(producer, topic_name, repo_data):
    for repo in repo_data:
        try:
            producer.produce(topic_name, key=str(repo['id']), value=json.dumps(repo))
            print(f"Sent repository: {repo['name']} to Kafka")
        except KafkaException as e:
            print(f"Failed to produce message: {e}")
            pass
    producer.flush()

# 请替换掉你的kafka ip和port
kafka_ip_port = 'your kafka'

admin_client = AdminClient({'bootstrap.servers': kafka_ip_port})
topic_name = 'github_repos'

existing_topics = admin_client.list_topics().topics

if topic_name not in existing_topics:
    topic_list = [NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

producer_conf = {
    'bootstrap.servers': kafka_ip_port,
}

producer = Producer(producer_conf)
print('Start!')
x = 1
while True:
    for page in range(1,11):
        recent_repos = []
        print("ok")
        repositories_data = get_repositories(page,x)
        if repositories_data:
            for repo in repositories_data['items']:
                temp = get_repository_info(repo)
        #  print(temp)
                recent_repos.append(temp)


        print(recent_repos)
        print(len(recent_repos))

        produce_to_kafka(producer, topic_name, recent_repos)
        print(page)

        print("Waiting for 1 minute before sending data again...")
        time.sleep(60)
    x += 1
    if 3*365- x*60<=0:
        break

