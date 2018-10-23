import dqo
import asyncpg

dqo.ASYNC_DB = dqo.Database(
  src=asyncpg.create_pool(database='responder_webapp')
)

@dqo.Table()
class Tweet:
  id = dqo.Column(int, primary_key=True)
  text = dqo.Column(str)

