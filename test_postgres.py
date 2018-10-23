import os, unittest

import psycopg2
import asyncpg

import dqo

from test_sync import BaseSync
from test_async import BaseAsync, async_test

class PostgresSync(BaseSync, unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    os.system('createdb dqo_test')
    dqo.SYNC_DB = dqo.Database(
      src=lambda: psycopg2.connect("dbname='dqo_test'"),
    )
    with dqo.SYNC_DB.connection as conn:
      conn.sync_execute('create table something (col1 integer, col2 text)', [])
    
  @classmethod
  def tearDownClass(cls):
    os.system('dropdb dqo_test')


class PostgresAsync(BaseAsync, unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    os.system('createdb dqo_test')
    dqo.ASYNC_DB = dqo.Database(
      src=lambda: asyncpg.connect(database='dqo_test')
    )
    @async_test
    async def f(self):
      async with dqo.ASYNC_DB.connection as conn:
        await conn.async_execute('create table something (col1 integer, col2 text)', [])
    f(None)
    
  @classmethod
  def tearDownClass(cls):
    os.system('dropdb dqo_test')


class PostgresAsyncPooled(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    os.system('createdb dqo_test')
    
  @classmethod
  def tearDownClass(cls):
    os.system('dropdb dqo_test')

  @async_test
  async def test_first(self):

    pool = asyncpg.create_pool(database='dqo_test')
    dqo.ASYNC_DB = dqo.Database(src=pool)
    async with dqo.ASYNC_DB.connection as conn:
      await conn.async_execute('create table something (col1 integer, col2 text)', [])

    @dqo.Table()
    class Something:
      col1 = dqo.Column(int)
      col2 = dqo.Column(str)

    await Something.ALL.insert(col1=1)
    self.assertEqual((await Something.ALL.first()).col1, 1)

    await pool.close()



if __name__ == '__main__':
    unittest.main()



