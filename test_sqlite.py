import os, unittest

import sqlite3
import aiosqlite

import dqo

from test_sync import BaseSync
from test_async import BaseAsync, async_test
from test_evolve import BaseEvolve

class SQLiteSync(BaseSync, unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.db = dqo.Database(
      sync_src=lambda: sqlite3.connect('dqo_test.db', isolation_level=None),
    )
    super().setUpClass()
    
  @classmethod
  def tearDownClass(cls):
    os.remove('dqo_test.db')

  def test_outer_join(self):
    pass

  def test_right_join(self):
    pass


class SQLiteAsync(BaseAsync): # unittest.TestCase

  @classmethod
  def setUpClass(cls):
    os.system('createdb dqo_test')
    cls.db = dqo.Database(
      sync_src=lambda: sqlite3.connect('dqo_test.db', isolation_level=None),
      async_src=lambda: aiosqlite.connect('dqo_test.db', isolation_level=None)
    )
    super().setUpClass()
    
  @classmethod
  def tearDownClass(cls):
    os.remove('dqo_test.db')


class SQLiteEvolve(BaseEvolve): # unittest.TestCase

  def setUp(self):
    super().setUp()
    
  def tearDown(self):
    os.remove('dqo_test.db')
  
  def build_db(self):
    return dqo.Database(
      sync_src=lambda: sqlite3.connect('dqo_test.db'),
    )


if __name__ == '__main__':
    unittest.main()



