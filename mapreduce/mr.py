import json
from mrjob.job import MRJob
from mrjob.step import MRStep
from statistics import median
import happybase


class WordCounter(MRJob):
    
    def mapper(self, _, line):
        post = json.loads(line)

        account = post["Account"]["username"]
        account_created_at = post["Account"]["created_at"]
        account_url = post["Account"]["url"]
        account_followers_count = post["Account"]["followers_count"]
        account_following_count = post["Account"]["following_count"]
        account_status_count = post["Account"]["statuses_count"]
        post_favourites_count = post["favourites_count"]
        post_reblogs_count = post["reblogs_count"]
        post_visibility = post["visibility"]
        post_media = post["media_attachments"]
        language = post["language"]
        post_tags = post["tags"]
        
        # User values
        yield "user:"+account, 1
        yield "created_at:"+str(account_created_at), 1
        yield "followers_count:"+account, account_followers_count
        yield "following_count:"+account, account_following_count
        yield "status_count:"+account, account_status_count
        # Post values
        yield "favourites_count:"+account, post_favourites_count
        yield "reblogs_count:"+account, post_reblogs_count
        yield "visibility:"+post_visibility, 1
        if language:
            yield "language:"+language, 1
        if post_tags:
            for tag in post_tags:
                yield "tag:"+tag["name"], 1
        # Other values
        yield "url:"+account_url, 1
        yield "media:", 1 if len(post_media) > 0 else 0


    def reducer(self, key, values):
        if "count" in str(key):
            yield key, max(values)
        else:
            yield key, sum(values)

    def reducer(key, values):

        # Establish a connection to HBase
        connection = happybase.Connection('localhost', port=16010)
        table_name = 'ns_mastodon:mastodon'
        table = connection.table(table_name)

        # Process the key-value pairs from the mapper function
        if key.startswith("user:") :
            # Save data to HBase
            column_family = 'cf_user'
            column = key.split(':')[0]
            value = values[0]
            table.put('row_key', {f'{column_family}:{column}': str(value)})
        elif key.startswith("followers_count:") or key.startswith("following_count:") or key.startswith("status_count:") or key.startswith("favourites_count:") or key.startswith("reblogs_count:"):
            # Save data to HBase
            column_family = 'cf_user'
            column = key.split(':')[0]
            account = key.split(':')[1]
            value = values[0]
            table.put(f'user:{account}', {f'{column_family}:{column}': str(value)})
        elif key.startswith("visibility:") or key.startswith("language:") or key.startswith("created_at:"):
            # Save data to HBase
            column_family = 'cf_post'
            column = key.split(':')[0]
            value = values[0]
            table.put('row_key', {f'{column_family}:{column}': str(value)})
        elif key.startswith("media:") or key.startswith("url:"):
            # Save data to HBase
            column_family = 'cf_other'
            column = key.split(':')[0]
            value = values[0]
            table.put('row_key', {f'{column_family}:{column}': str(value)})
        elif key.startswith("tag:"):
            # Save data to HBase
            column_family = 'cf_post'
            column = key.split(':')[0]
            tag_name = key.split(':')[1]
            value = values[0]
            table.put(f'tag:{tag_name}', {f'{column_family}:{column}': str(value)})

        # Close the HBase connection
        connection.close()

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]


if __name__ == '__main__':
    WordCounter.run()
        