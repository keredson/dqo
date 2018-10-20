import os, unittest

import psycopg2
import asyncpg

import dqo

from test_base import BaseDBSync, BaseDBAsync, async_test

class PostgresSync(BaseDBSync, unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    os.system('createdb dqo_test')
    dqo.DEFAULT_SYNC_DB = dqo.Database(
      dialect=dqo.Dialect.POSTGRES('10'), 
      module=psycopg2,
      src=lambda: psycopg2.connect("dbname='dqo_test'"),
    )
    with dqo.DEFAULT_SYNC_DB.connection as conn:
      conn.sync_execute('create table something (col1 integer, col2 text)', [])
    
  @classmethod
  def tearDownClass(cls):
    os.system('dropdb dqo_test')


class PostgresAsync(BaseDBAsync, unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    os.system('createdb dqo_test')
    dqo.DEFAULT_ASYNC_DB = dqo.Database(
      dialect=dqo.Dialect.POSTGRES('10'),
      module=asyncpg, 
      src=lambda: asyncpg.connect(database='dqo_test')
    )
    @async_test
    async def f(self):
      async with dqo.DEFAULT_ASYNC_DB.connection as conn:
        await conn.async_execute('create table something (col1 integer, col2 text)', [])
    f(None)
    
  @classmethod
  def tearDownClass(cls):
    os.system('dropdb dqo_test')


if __name__ == '__main__':
    unittest.main()

