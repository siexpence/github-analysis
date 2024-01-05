import json
import requests
import time
import argparse
import logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from dateutil.parser import parse
from datetime import datetime


def parse_issue_item(repo_owner, repo_name, issue):
    ret = {
        "id": issue["id"],
        "repo_name": repo_name,
        "repo_owner": repo_owner,
        "author": issue["user"].get("login", ""),
        "author_association": issue["author_association"],
        "comments": issue["comments"],
        "state": issue["state"],
        "title": issue["title"],
        "body": issue["body"],
        "created_at": issue["created_at"],
        "updated_at": issue["updated_at"],
        "closed_at": issue["closed_at"],
    }
    if issue["closed_at"]:
        finish_time = parse(issue["closed_at"]) - parse(issue["created_at"])
        ret["finish_time"] = int(finish_time.total_seconds()) / 3600
    else:
        ret["finish_time"] = -1
    if issue["assignee"]:
        ret["assignee"] = issue["assignee"].get("login", "")
    else:
        ret["assignee"] = ""
    return ret


id_cache = set()


def fetch_issues(owner, repo, since_time, producer, topic_name, starting_date):
    """Fetch all issues from GitHub API with pagination."""
    issues = []
    page = 0
    while True:
        headers = {"Authorization": f"Bearer <your-token>"}
        url = f"https://api.github.com/repos/{owner}/{repo}/issues"
        params = {
            "state": "all",
            "sort": "created",
            "direction": "desc",
            "page": page,
            "per_page": 100,  # Maximum allowed per page
        }
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            new_issues = response.json()
            if not new_issues:
                break  # No more new commits
            print(f"Fetched {len(new_issues)} issues from {owner}/{repo}")
            new_issues_parsed = [parse_issue_item(owner, repo, i) for i in new_issues]
            new_issues_satisfying_time = [
                i for i in new_issues_parsed if i["created_at"] > since_time
            ]
            new_issues_dedup = [
                i for i in new_issues_satisfying_time if i["id"] not in id_cache
            ]
            for i in new_issues_dedup:
                print(i["created_at"], since_time, i["title"])
            id_cache.update([i["id"] for i in new_issues_dedup])
            for issue in new_issues_dedup:
                producer.send(topic_name, issue)
                assert parse(issue["created_at"]) >= parse(starting_date)
                print(f"Sent issue: {issue['id']} from {repo}")
            issues.extend(new_issues_dedup)
            if len(new_issues_dedup) == 0:
                break
            else:
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

    return issues


def produce_commits(producer, topic_name, repo_list, sleep_seconds, starting_date):
    """Produce commits to Kafka topic with given sleep interval."""
    while True:
        inactive_cnt = 0
        for repo in repo_list:
            repo_name = repo["repo_name"]
            issues = fetch_issues(
                repo["owner"],
                repo_name,
                starting_date,
                producer,
                topic_name,
                starting_date,
            )
            if len(issues) == 0:
                inactive_cnt += 1

        if inactive_cnt == len(repo_list):
            print("sleeping")
            time.sleep(sleep_seconds)


def main():
    parser = argparse.ArgumentParser(description="GitHub Issues Kafka Producer")
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
    # starting_date = parse(args.starting_date)

    # Initialize Kafka Producer
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    topic_name = "list_issues_v0"

    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Start producing commits to Kafka
    produce_commits(
        producer, topic_name, repositories, args.sleep_seconds, args.starting_date
    )


if __name__ == "__main__":
    main()
