import os, unittest

import psycopg2

import dqo

from test_base import BaseDB

class Postgres(BaseDB, unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    os.system('createdb dqo_test')
    dqo.DEFAULT_DB = dqo.Database(
      dialect=dqo.Dialect.POSTGRES('10'), 
      sync_src=lambda: psycopg2.connect("dbname='dqo_test'")
    )
    with dqo.DEFAULT_DB.connection as conn:
      conn.sync_execute('create table something (col1 integer, col2 text)', [])
    
  @classmethod
  def tearDownClass(cls):
    os.system('dropdb dqo_test')


if __name__ == '__main__':
    unittest.main()

