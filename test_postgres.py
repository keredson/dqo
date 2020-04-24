import os, unittest

import psycopg2
import asyncpg

import dqo

from test_sync import BaseSync
from test_async import BaseAsync, async_test
from test_evolve import BaseEvolve

class PostgresSync(BaseSync, unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    os.system('createdb dqo_test')
    cls.db = dqo.Database(
      sync_src=lambda: psycopg2.connect("dbname='dqo_test'"),
    )
    super().setUpClass()
    
  @classmethod
  def tearDownClass(cls):
    os.system('dropdb dqo_test')


class PostgresAsync(BaseAsync, unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    os.system('createdb dqo_test')
    cls.db = dqo.Database(
      sync_src=lambda: psycopg2.connect("dbname='dqo_test'"),
      async_src=lambda: asyncpg.connect(database='dqo_test')
    )
    super().setUpClass()
    
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
    dqo.DB = dqo.Database(async_src=pool, async_dialect=dqo.Dialect.POSTGRES(10, lib=asyncpg))
    async with dqo.DB.connection() as conn:
      await conn.async_execute('create table something (col1 integer, col2 text)', [])

    @dqo.Table()
    class Something:
      col1 = dqo.Column(int)
      col2 = dqo.Column(str)

    await Something.ALL.insert(col1=1)
    self.assertEqual((await Something.ALL.first()).col1, 1)

    await pool.close()


class PostgresEvolve(BaseEvolve, unittest.TestCase):

  pk_int = 'serial'

  def setUp(self):
    os.system('createdb dqo_test')
    super().setUp()
    
  def tearDown(self):
    os.system('dropdb dqo_test')
  
  def build_db(self):
    return dqo.Database(
      sync_src=lambda: psycopg2.connect("dbname='dqo_test'"),
    )


if __name__ == '__main__':
    unittest.main()



