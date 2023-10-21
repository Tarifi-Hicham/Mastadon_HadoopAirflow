import json
from mrjob.job import MRJob
from mrjob.step import MRStep
from statistics import median


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
        
        yield "user:"+account, 1
        yield "created_at:"+str(account_created_at), 1
        yield "url:"+account_url, 1
        yield "followers_count:"+account, account_followers_count
        yield "following_count:"+account, account_following_count
        yield "status_count:"+account, account_status_count
        yield "favourites_count:"+account, post_favourites_count
        yield "reblogs_count:"+account, post_reblogs_count
        yield "visibility:"+post_visibility, 1
        yield "language"+language, 1
        yield "media", 1 if len(post_media) > 0 else 0
        # for tag in post_tags:
            # yield "tag:"+tag, 1

    def reducer(self, key, values):
        if "count" in str(key):
            yield key, max(values)
        else:
            yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]


if __name__ == '__main__':
    WordCounter.run()
        