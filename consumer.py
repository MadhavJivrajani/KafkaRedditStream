from kafka import KafkaConsumer
from json import loads, dumps
import requests
import threading
from time import sleep

class StreamReddit:
    def __init__(
            self, subreddit, limit=50, author=True,
            comments=False, url=True, name=False,
            num_comments=False, score=False,
            title=True, created_utc=False, edited=False,
            spoiler=False
    ):

        """
        subreddit    : (str)  subreddit whose posts will be fetched
        limit        : (int)  top <limit> number of posts in the corresponding subreddit
        author       : (bool) whether to include author of each post in the returned JSON or not
        comments     : (bool) whether to include comments of each post in the returned JSON or not
        url          : (bool) whether to include url or permalink (if self post) in the returned JSON or not
        name         : (bool) whether to include the full name of each post in the returned JSON or not
        num_comments : (bool) whether to inlcude number of comments of each post in the returned JSON or not
        score        : (bool) whether to include total number of upvotes for each post in the returned JSON or not
        title        : (bool) whether to include title of each post in the returned JSON or not
        created_utc  : (bool) include the time (Unix Time) at which each post was created in the returned JSON or not
        edited       : (bool) include whether the post was edited or not in the returned JSON or not
        spoiler      : (bool) include whether the post was tagged as a 'spoiler' or not in the returned JSON
        """
        self.args = locals()
        del self.args['self']

    def send_data(self):
        r = requests.post("http://127.0.0.1:5000",
                          data = dumps(self.args).encode('utf-8'))
        return r.status_code

    def get_stream(self):    
        consumer = KafkaConsumer(
            bootstrap_servers = ['localhost:9092'],
            auto_offset_reset = 'latest', #change to 'earliest' for getting a stream of historical data as well
            enable_auto_commit = True,
            value_deserializer = lambda x : loads(x.decode('utf-8'))
        )

        consumer.subscribe(['redditStream']) #redditStream -> name of created topic

        for message in consumer:
            print(message.value)             


stream = StreamReddit("MineCraft", comments = False, limit=20, num_comments = "True")

send_thread = threading.Thread(target = stream.send_data)
send_thread.start()

sleep(2)

stream_thread = threading.Thread(target = stream.get_stream)
stream_thread.start()

send_thread.join()
stream_thread.join()
