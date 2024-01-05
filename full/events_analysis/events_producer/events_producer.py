import json
import requests
import time
import argparse
import logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from dateutil.parser import parse
from datetime import datetime

# 从github API获取event数据, 并作为producer将数据发送到kafka
access_token = '<your-token>'
topic_name_all_events = "all_events_list"  # 主题
topic_name_repo_events = "repo_events_list"


# 获取一段时间内所有的events 数据
def get_all_events(last_fetched_time):
    page = 1
    events = []
    while True:
        url = "https://api.github.com/events"  # 一段时间内的所有事件
        headers = {'Authorization': f'token {access_token}'}
        params = {
            'since': last_fetched_time.isoformat() + 'Z',  # ISO 8601 format with Z
            'page': page,
            'per_page': 100  # Maximum allowed per page
        }
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            new_events = response.json()  # 转化为json文件
            if not new_events:
                break  # No more new events
            print(f"Fetched {len(new_events)} events from pubilc")
            events.extend(new_events)
            page += 1
        else:
            print(f"Error fetching data for all public events: {response.status_code}")
            break

        # Handle rate limiting  速率限制
        if 'X-RateLimit-Remaining' in response.headers and int(response.headers['X-RateLimit-Remaining']) == 0:
            reset_time = int(response.headers['X-RateLimit-Reset'])
            sleep_duration = max(reset_time - time.time(), 0)
            print(f"Rate limit reached. Sleeping for {sleep_duration} seconds.")
            time.sleep(sleep_duration)
    
    if events:
        last_fetched_time = parse(events[0]['created_at']) 

    return events, last_fetched_time

# 爬取指定名称的事件
def get_repo_events(owner, repo, last_fetched_time):
    page = 1
    events = []
    while True:
        url = f"https://api.github.com/networks/{owner}/{repo}/events"
        headers = {'Authorization': f'token {access_token}'}
        params = {'per_page': 100}  # Maximum allowed per page
        params = {
            'since': last_fetched_time.isoformat() + 'Z',  # ISO 8601 format with Z
            'page': page,
            'per_page': 100  # Maximum allowed per page
        }
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            new_events = response.json()  # 转化为json文件
            if not new_events:
                break  # No more new events
            print(f"Fetched {len(new_events)} events from {owner}/{repo}")
            events.extend(new_events)
            page += 1
        else:
            print(f"Error fetching data for {owner}/{repo}: {response.status_code}")
            break

        # Handle rate limiting  速率限制
        if 'X-RateLimit-Remaining' in response.headers and int(response.headers['X-RateLimit-Remaining']) == 0:
            reset_time = int(response.headers['X-RateLimit-Reset'])
            sleep_duration = max(reset_time - time.time(), 0)
            print(f"Rate limit reached. Sleeping for {sleep_duration} seconds.")
            time.sleep(sleep_duration)
    # events不为空
    if events:
        last_fetched_time = parse(events[0]['created_at']) 

    return events, last_fetched_time

# 获取上次爬取到的最后的event的时间
def get_last_event_time():
    file_name = 'event_list.json'
    try:
        with open(file_name, 'r') as file:
            data = json.load(file)
            if data:
                last_event = data[-1]
                last_event_time = last_event.get('created_at', 0)
                return last_event_time
            else:
                return 0
    except FileNotFoundError:
        return 0
    except json.JSONDecodeError:
        return 0


# 获取某一时间段内所有event的数据，并将事件数据传到kafka
def produce_all_events(producer, topic_name, starting_date):
    """Produce events to Kafka topic with given sleep interval."""
    pre_last_fetched_time = get_last_event_time()
    last_fetched_time = starting_date
    while True:
        inactive_cnt = 0
        events, last_fetched_time = get_all_events(last_fetched_times)
        if last_fetched_time == pre_last_fetched_time:
            inactive_cnt += 1  # 没有更新
            print('sleeping')
            time.sleep(sleep_seconds)
        else:
            pre_last_fetched_time = last_fetched_time # 更新下次获取的时间
            for event in events:
                producer.send(topic_name, event)
        

# 根据event_list获取指定event的数据，并将事件数据传到kafka
def produce_repo_events(producer, topic_name, repo_list, sleep_seconds, last_fetched_times):
    """Produce events to Kafka topic with given sleep interval."""
    while True:
        inactive_cnt = 0
        for repo in repo_list:
            repo_name = repo['repo_name']
            events, last_fetched_time = fetch_repo_events(repo['owner'], repo_name, last_fetched_times[repo_name])
            if last_fetched_time == last_fetched_times[repo_name]:
                inactive_cnt += 1  # 没有更新
            else:
                last_fetched_times[repo_name] = last_fetched_time # 更新下次获取的时间
                for event in events:
                    producer.send(topic_name, event)
        
        # 都没有更新的话，就睡眠等待
        if inactive_cnt == len(repo_list):
            print('sleeping')
            time.sleep(sleep_seconds)


def main():
    parser = argparse.ArgumentParser(description='GitHub Commits Kafka Producer')
    parser.add_argument('--all_events', type=int, help='Whether to get all events', default= 1)
    parser.add_argument('--repo_list_path', type=str, help='Path to JSON file containing list of repositories', default='./repo_list.json')
    parser.add_argument('--sleep_seconds', type=int, help='Seconds to sleep between each crawl', default= 3600)
    parser.add_argument('--starting_date', type=str, help='Starting date in ISO 8601 format', default= "2021-01-01T0:00:00")

    args = parser.parse_args()

    # Parse the starting date
    starting_date = parse(args.starting_date)  

    # Initialize Kafka Producer
    admin_client = KafkaAdminClient(bootstrap_servers="<your-ip-here>:<your-port-here>")  # localhost
    
    existing_topics = admin_client.list_topics() 

    if topic_name not in existing_topics:
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    producer = KafkaProducer(bootstrap_servers='<your-ip-here>:<your-port-here>',
                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Start producing commits to Kafka
    if all_events:
        produce_all_events(producer, topic_name_all_events, starting_date)
    else:
        # Read the list of repositories
        with open(args.repo_list_path, 'r') as file:
            repositories = json.load(file)
        # Initialize the last fetched time for each repository
        last_fetched_times = {repo['repo_name']: starting_date for repo in repositories}
        produce_repo_events(producer, topic_name_repo_events, repositories, args.sleep_seconds, last_fetched_times)


if __name__ == '__main__':
    main()

