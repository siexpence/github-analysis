import json
import requests
import time
import argparse
import logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from dateutil.parser import parse
from datetime import datetime


def parse_commit_item(repo_owner, repo_name, item):
    ret = {
        "repo_owner": repo_owner,
        "repo_name": repo_name,
        "sha": item["sha"],
        "commit": {
            "author": {
                "name": item["commit"]["author"]["name"],
                "email": item["commit"]["author"]["email"],
                "date": item["commit"]["author"]["date"],
            },
            "committer": {
                "name": item["commit"]["committer"]["name"],
                "email": item["commit"]["committer"]["email"],
                "date": item["commit"]["committer"]["date"],
            },
            "message": item["commit"]["message"],
        },
    }
    if "committer" in item and item["committer"] is not None:
        ret["committer"] = {
            "login": item["committer"].get("login", ""),
            "id": item["committer"].get("id", ""),
            "node_id": item["committer"].get("node_id", ""),
            "html_url": item["committer"].get("html_url", ""),
        }
    else:
        ret["committer"] = {
            "login": "",
            "id": "",
            "node_id": "",
            "html_url": "",
        }
    if "author" in item and item["author"] is not None:
        ret["author"] = {
            "login": item["author"].get("login", ""),
            "id": item["author"].get("id", ""),
            "node_id": item["author"].get("node_id", ""),
            "html_url": item["author"].get("html_url", ""),
        }
    else:
        ret["author"] = {
            "login": "",
            "id": "",
            "node_id": "",
            "html_url": "",
        }
    return ret


def fetch_commits(owner, repo, last_fetched_time):
    """Fetch all commits from GitHub API with pagination."""
    commits = []
    page = 1
    while True:
        headers = {"Authorization": f"Bearer <your-token>"}
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        params = {
            "since": last_fetched_time.isoformat() + "Z",  # ISO 8601 format with Z
            "page": page,
            "per_page": 100,  # Maximum allowed per page
        }
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            new_commits = response.json()
            if not new_commits:
                break  # No more new commits
            print(f"Fetched {len(new_commits)} commits from {owner}/{repo}")
            commits.extend(new_commits)
            page += 1
        else:
            print(f"Error fetching data for {owner}/{repo}: {response.status_code}")
            break

        # Handle rate limiting
        if (
            "X-RateLimit-Remaining" in response.headers
            and int(response.headers["X-RateLimit-Remaining"]) == 0
        ):
            reset_time = int(response.headers["X-RateLimit-Reset"])
            sleep_duration = max(reset_time - time.time(), 0)
            print(f"Rate limit reached. Sleeping for {sleep_duration} seconds.")
            time.sleep(sleep_duration)

    # Determine the most recent commit time for the next request
    if commits:
        last_fetched_time = parse(commits[0]["commit"]["committer"]["date"])

    return commits, last_fetched_time


def produce_commits(producer, topic_name, repo_list, sleep_seconds, last_fetched_times):
    """Produce commits to Kafka topic with given sleep interval."""
    while True:
        inactive_cnt = 0
        for repo in repo_list:
            repo_name = repo["repo_name"]
            commits, last_fetched_time = fetch_commits(
                repo["owner"], repo_name, last_fetched_times[repo_name]
            )
            if last_fetched_time == last_fetched_times[repo_name]:
                inactive_cnt += 1
            else:
                last_fetched_times[repo_name] = last_fetched_time
                for commit in commits:
                    parsed_commit = parse_commit_item(repo["owner"], repo_name, commit)
                    producer.send(topic_name, parsed_commit)
                    print(
                        f"Sent commit: {commit['sha']} from {repo_name}, {last_fetched_time}"
                    )
                    # print(json.dumps(parsed_commit, indent=2, ensure_ascii=False))

        if inactive_cnt == len(repo_list):
            print("sleeping")
            time.sleep(sleep_seconds)


def main():
    parser = argparse.ArgumentParser(description="GitHub Commits Kafka Producer")
    parser.add_argument(
        "repo_list_path",
        type=str,
        help="Path to JSON file containing list of repositories",
    )
    parser.add_argument(
        "sleep_seconds", type=int, help="Seconds to sleep between each crawl"
    )
    parser.add_argument(
        "starting_date", type=str, help="Starting date in ISO 8601 format"
    )

    args = parser.parse_args()

    # Read the list of repositories
    with open(args.repo_list_path, "r") as file:
        repositories = json.load(file)

    # Parse the starting date
    starting_date = parse(args.starting_date)

    # Initialize Kafka Producer
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    topic_name = "list_commits_cls_test"

    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Initialize the last fetched time for each repository
    last_fetched_times = {repo["repo_name"]: starting_date for repo in repositories}

    # Start producing commits to Kafka
    produce_commits(
        producer, topic_name, repositories, args.sleep_seconds, last_fetched_times
    )


if __name__ == "__main__":
    main()
