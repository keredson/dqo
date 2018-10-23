import dqo
import responder

from models import *

api = responder.API()

@api.route("/tweets")
async def tweets(req, resp):
  latest_tweets = [t.text async for t in Tweet.ALL.order_by(Tweet.id.desc).limit(100)]
  resp.text = '\n'.join(latest_tweets)

@api.route("/tweet/{text}")
async def tweet(req, resp, *, text):
  await Tweet(text=text).insert()
  resp.text = 'ok'

if __name__ == '__main__':
    api.run()

