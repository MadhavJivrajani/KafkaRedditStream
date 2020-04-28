from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
from flask import Flask, Response, request
import praw

app = Flask(__name__)

@app.route('/', methods=['POST', 'GET'])
def get_data():
    config = loads(request.data.decode('utf-8'))
    data = {}
    
    r = praw.Reddit(user_agent = '<app name>',
                    client_id = '<client id>',
                    client_secret = '<client secret>'
    )
    
    submissions = r.subreddit(config['subreddit']).hot(limit=config['limit'])
    
    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],
                             value_serializer = lambda x : dumps(x).encode('utf-8')
    )
    
    del config['subreddit']
    del config['limit']

    if config['comments']:
        for submission in submissions:
            config['comments'] = True
            data[submission.id] = {}
            temp = ""
            for comment in submission.comments.list():
                try:
                    temp += comment.body
                    temp += '\n'
                except:
                    continue
            data[submission.id]['comments'] = temp
            
            config['comments'] = False
            for key in config:
                if config[key]:
                    if key == 'author':
                        data[submission.id][key] = vars(submission)[key].name
                    else:
                        data[submission.id][key] = vars(submission)[key]
            producer.send('redditStream', value = data[submission.id]) #redditStream -> name of created topic
            sleep(1)
    else:
        for submission in submissions:
            if submission.id not in data:
                data[submission.id] = {}
            
            for key in config:
                if config[key]:
                    if key == 'author':
                        data[submission.id][key] = vars(submission)[key].name
                    else:
                        data[submission.id][key] = vars(submission)[key]
            producer.send('redditStream', value = data[submission.id]) #redditStream -> name of created topic
            sleep(1)

    return 'OK', 200
            
if __name__ == '__main__':
    app.run(port=5000, debug=True)
