import json
from mrjob.job import MRJob
from mrjob.step import MRStep


class WordCounter(MRJob):
    
    def mapper(self, _, line):
        post = json.loads(line)

        account_id = post["Account"]["id"]
        username = post["Account"]["username"]
        account_created_at = post["Account"]["created_at"]
        account_url = post["Account"]["url"]
        account_followers_count = post["Account"]["followers_count"]
        account_following_count = post["Account"]["following_count"]
        account_status_count = post["Account"]["statuses_count"]
        post_id = post["post_id"]
        post_favourites_count = post["favourites_count"]
        post_reblogs_count = post["reblogs_count"]
        post_visibility = post["visibility"]
        post_media = post["media_attachments"]
        language = post["language"]
        post_tags = post["tags"]
        
        # User values
        yield "username:"+str(account_id), username
        yield "created_at:"+str(account_id), account_created_at
        yield "followers_count:"+str(account_id), account_followers_count
        yield "following_count:"+str(account_id), account_following_count
        yield "status_count:"+str(account_id), account_status_count
        if len(account_url) > 1:
            yield "url:"+str(account_id), account_url
        else:
            yield "url:"+str(account_id), "None"
        # Post values
        yield "user_id:"+str(post_id), account_id
        yield "favourites_count:"+str(post_id), post_favourites_count
        yield "reblogs_count:"+str(post_id), post_reblogs_count
        yield "visibility:"+str(post_id), post_visibility
        if language:
            yield "language:"+str(post_id), language
        else:
            yield "language:"+str(post_id), "None"
        if post_tags:
            for tag in post_tags:
                yield "tag:"+str(post_id), tag["name"]
        yield "media:"+str(post_id), 1 if len(post_media) > 0 else 0

    def combiner(self, key, values):
        yield key,max(values)

    def reducer(self, key, values):
        yield key, max(values)


    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]


if __name__ == '__main__':
    WordCounter.run()
        