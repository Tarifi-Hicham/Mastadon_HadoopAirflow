import pandas as pd
from datetime import datetime
from mastodon import Mastodon
from hdfs import InsecureClient
from dotenv import load_dotenv
import os


# this function will pass through to the HDFS client write()
def send_data_to_hdfs(df: pd.DataFrame):
    
    # Convert DataFrame to JSON format
    data = df.to_json(orient='records', lines=True)
    # json_data = json.dump(data)
    #json_bytes = data.encode('utf-8')

    user_name = 'TarifiHadoopAdmin'
    host = 'http://localhost:9870'

    # Connect to HDFS
    client = InsecureClient(host, user=user_name)

    hdfs_filepath = "/user/" + user_name + "/" + datetime.now().strftime('%d-%m-%Y') + "/posts" + datetime.now().strftime('%H%M%S') + ".json"

    # Upload the JSON data to HDFS
    with client.write(hdfs_filepath, overwrite=True) as hdfs_file:
        hdfs_file.write(data)


# Load environment variables from .env
load_dotenv()

# Create Mastodon API client
mastodon = Mastodon(
    client_id=os.getenv('Client_key'),
    client_secret=os.getenv('Client_secret'),
    access_token=os.getenv('Access_token'),
    api_base_url="https://mastodon.social"
)

def get_posts(id):
    # toots = mastodon.timeline_hashtag(query,limit=10000)
    toots = mastodon.timeline_public(limit=10000,since_id=id)
    data = []
    for toot in toots:
        posts_data = {
        'post_id': toot['id'],
        'account': toot['account'],
        'content': toot['content'],
        'created_at': toot['created_at'],
        'favourites_count': toot['favourites_count'],
        'reblogs_count': toot['reblogs_count'],
        'sensitive': toot['sensitive'],
        'visibility': toot['visibility'],
        'mentions': toot['mentions'],
        'media_attachments': toot['media_attachments'],
        'tags': toot['tags'],
        'emojis' : toot['emojis'],
        'language': toot['language'],
        }
        data.append(posts_data)
    return data

# iteration count 
count = 20

# Extract posts and save it to hdfs
print("Getting posts from API")

posts_data = []

for i in range(count):
    # Specify the last toot id
    with open('extract/last_toot_id.txt', 'r+') as file:
        last_toot_id = file.read().strip()

    if last_toot_id != '':
        last_toot_id = int(last_toot_id)
    else:
        last_toot_id = None

    # Get Data from Mastodon API
    data = get_posts(last_toot_id)
    posts_data += data

    # Save the last toot id
    with open('extract/last_toot_id.txt', 'r+') as file:
        file.write(str(posts_data[-1]['post_id']))
    # print(last_toot_id)

# Sending Files to hdfs
print("Sending file to hdfs")
df_posts = pd.DataFrame(posts_data)
send_data_to_hdfs(df_posts)
