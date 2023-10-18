import pandas as pd
from datetime import datetime
from mastodon import Mastodon
from hdfs import InsecureClient
from dotenv import load_dotenv
import os


# this function will pass through to the HDFS client write()
def send_data_to_hdfs(df: pd.DataFrame, data_type: str):
    # Convert DataFrame to JSON format
    json_data = df.to_json(orient='records')
    json_bytes = json_data.encode('utf-8')

    user_name = 'TarifiHadoopAdmin'
    host = 'http://localhost:9870'

    # Connect to HDFS
    client = InsecureClient(host, user=user_name)

    if data_type == 'users':
        hdfs_filepath = "/user/" + user_name + "/" + datetime.now().strftime('%d%m%Y') + "/users" + datetime.now().strftime('%H%M%S') + ".json"
    elif data_type == 'posts':
        hdfs_filepath = "/user/" + user_name + "/" + datetime.now().strftime('%d%m%Y') + "/posts" + datetime.now().strftime('%H%M%S') + ".json"

    # Upload the JSON data to HDFS
    with client.write(hdfs_filepath, overwrite=True) as hdfs_file:
        hdfs_file.write(json_bytes)


# Load environment variables from .env
load_dotenv()

# Get the access token (Create a .env file and add access_token variable in it)
access_token = os.getenv('access_token')

# Create Mastodon API client
mastodon = Mastodon(
    api_base_url = 'https://mastodon.social',
    access_token = access_token,
)

def search_users():
    data = []
    for char in 'abcdefghijklmnopqrstuvwxyz':
        results = mastodon.account_search(q=char)
        for user in results:
            user_data = {
                'id': user['id'],
                'username': user['username'],
                'display_name': user['display_name'],
                'is_group': user['group'],
                'following_count': user['following_count'],
                'followers_count': user['followers_count'],
                'statuses_count': user['statuses_count'],
                'bio': user['note'],
                'is_bot': user['bot'],
                'created_at': user['created_at']
            }
            data.append(user_data)
    return data

def get_posts():
    # toots = mastodon.timeline_hashtag(query,limit=10000)
    toots = mastodon.timeline_public(limit=10000)
    data = []
    for toot in toots:
        posts_data = {
        'post_id': toot['id'],
        'Account_id': toot['account']['id'],
        'Content': toot['content'],
        'Created_at': toot['created_at'],
        'favourites_count': toot['favourites_count'],
        'sensitive': toot['sensitive'],
        'visibility': toot['visibility'],
        'mentions': toot['mentions'],
        'media_attachments': toot['media_attachments'],
        'tags': toot['tags'],
        'language': toot['language'],
        }
        data.append(posts_data)
    return data


# Extract users and save it to hdfs
print("Getting users from API")
users_data = search_users()  # search for users by a letter
df_users = pd.DataFrame(users_data)

# Sending Files to hdfs
print("Sending file to hdfs")
send_data_to_hdfs(df_users, 'users')

# Extract posts and save it to hdfs
print("Getting posts from API")
# Extract posts and save it to hdfs
posts_data = []
for i in range(100):
    posts = get_posts()  # Get the toots
    for post in posts:
        posts_data.append(post)

# Sending Files to hdfs
print("Sending file to hdfs")
df_posts = pd.DataFrame(posts_data)
send_data_to_hdfs(df_posts, 'posts')
