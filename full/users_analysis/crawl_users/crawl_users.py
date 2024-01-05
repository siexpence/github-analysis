from confluent_kafka import Producer
import json
import requests
import time

access_token = <your-token>

def get_last_user_id():
    file_name = 'github_users.json'
    try:
        with open(file_name, 'r') as file:
            data = json.load(file)
            if data:
                last_user = data[-1]
                last_user_id = last_user.get('id', 0)
                return last_user_id
            else:
                return 0
    except FileNotFoundError:
        return 0
    except json.JSONDecodeError:
        return 0

def get_github_users(last_id):
    endpoint = 'https://api.github.com/users'
    headers = {'Authorization': f'token {access_token}'}
    params = {'per_page': 20, 'since': last_id}

    response = requests.get(endpoint, headers=headers, params=params)

    if response.status_code == 200:
        users = response.json()

        with open('github_users.json', 'w') as file:
            json.dump(users, file, indent=4)
        print('User information has been saved to github_users.json')

        user_name_list = [user['login'] for user in users]
        return user_name_list
    else:
        print(f'Error: {response.status_code} - {response.text}')

def get_user_info(username):
    endpoint = f'https://api.github.com/users/{username}'
    headers = {'Authorization': f'token {access_token}'}

    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        user_info = response.json()
        return user_info
    else:
        print(f'Error for {username}: {response.status_code} - {response.text}')
        return None

def get_all_users_info(user_name_list):
    # filename = 'all_users_info.json'
    user_info_list = []

    for user_name in user_name_list:
        user_info = get_user_info(user_name)
        if user_info:
            user_info_list.append(user_info)

    # with open(filename, 'w') as file:
    #     json.dump(userinfo_list, file, indent=4)
    # print('User information has been saved to all_users_info.json')

    return user_info_list

def send_users_info_to_kafka(user_info_list):
    kafka_server = <your-kafka-server-ip>
    topic_name = 'github-users-0'
    producer_config = {
        'bootstrap.servers': kafka_server,
    }
    producer = Producer(producer_config)

    for user_info in user_info_list:
        json_data = json.dumps(user_info).encode()
        key_bytes = str(user_info['id']).encode()
        producer.produce(topic_name, key=key_bytes, value=json_data)
    print('User information has been sent to kafka')

    producer.flush()

def get_rate_limit(username, access_key):
    url = 'https://api.github.com/rate_limit'
    response = requests.get(url, auth=(username, access_key))
    rate_limit_data = response.json()['resources']['core']
    return rate_limit_data['limit'], rate_limit_data['remaining'], rate_limit_data['reset']

def check_and_wait(username, access_key):
    limit, remaining, reset_time = get_rate_limit(username, access_key)

    print(f"Limit: {limit}, Remaining: {remaining}, Reset Time: {reset_time}")

    if remaining < 1000:
        sleep_time = reset_time - int(time.time())
        print(f"Sleeping for {sleep_time} seconds until reset.")
        time.sleep(sleep_time)

if __name__ == "__main__":
    while True:
        try:
            check_and_wait(<username>, <your-token>)
            last_id = get_last_user_id()
            user_name_list = get_github_users(last_id)
            user_info_list = get_all_users_info(user_name_list)
            send_users_info_to_kafka(user_info_list)
            time.sleep(600)
        except Exception as e:
            print(f"Error fetching/publishing events: {e}")
