import unittest
import asyncio

import dqo

from test_models import *

def async_test(af):
  def test_f(self):
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop) # needed?
    def f():
      return af(self)
    coro = asyncio.coroutine(f)
    event_loop.run_until_complete(coro())
    event_loop.close()
  test_f.__name__ = af.__name__
  return test_f

class GenericDB(unittest.TestCase):

  def test_select_all(self):
    for something in Something.ALL:
      self.assertEqual(something, 1)

  @async_test
  async def test_select_all_async(self):
    async for something in Something.ALL:
      self.assertEqual(something, 1)

  def test_first(self):
    self.assertEqual(Something.ALL.first(), 1)

  @async_test
  async def test_first_async(self):
    self.assertEqual(await Something.ALL.first(), 1)

  def test_len(self):
    self.assertEqual(len(Something.ALL), 1)

  @async_test
  async def test_len_async(self):
    self.assertEqual(len(Something.ALL), 1)


class GenericSQL(unittest.TestCase):

  def test_select_all(self):
    sql, args = Something.ALL._sql()
    self.assertEqual(sql, 'select col1,col2 from something')
    self.assertEqual(args, [])

  def test_select_also_already_selected_col(self):
    sql, args = Something.ALL.select(+Something.col2)._sql()
    self.assertEqual(sql, 'select col1,col2 from something')
    self.assertEqual(args, [])

  def test_select_col1(self):
    sql, args = Something.ALL.select(Something.col1)._sql()
    self.assertEqual(sql, 'select col1 from something')
    self.assertEqual(args, [])

  def test_select_not_col1(self):
    sql, args = Something.ALL.select(-Something.col1)._sql()
    self.assertEqual(sql, 'select col2 from something')
    self.assertEqual(args, [])

  def test_select_nothing(self):
    sql, args = Something.ALL.select()._sql()
    self.assertEqual(sql, 'select  from something') # not valid sql
    self.assertEqual(args, [])

  def test_select_add_col2(self):
    sql, args = Something.ALL.select().select(+Something.col2)._sql()
    self.assertEqual(sql, 'select col2 from something')
    self.assertEqual(args, [])

  def test_where(self):
    sql, args = Something.ALL.where(Something.col1==1)._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=?')
    self.assertEqual(args, [1])

  def test_where_kwargs(self):
    sql, args = Something.ALL.where(col1=1)._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=?')
    self.assertEqual(args, [1])

  def test_where_args_and_kwargs(self):
    sql, args = Something.ALL.where(Something.col1==1, col2=2)._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=? and col2=?')
    self.assertEqual(args, [1,2])

  def test_and(self):
    sql, args = Something.ALL.where((Something.col1==1) & (Something.col2==2))._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=? and col2=?')
    self.assertEqual(args, [1,2])

  def test_or(self):
    sql, args = Something.ALL.where((Something.col1==1) | (Something.col2==2))._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=? or col2=?')
    self.assertEqual(args, [1,2])

  def test_nested_conditional(self):
    sql, args = Something.ALL.where((Something.col1==1) | (Something.col2==2), col1=3)._sql()
    self.assertEqual(sql, 'select col1,col2 from something where (col1=? or col2=?) and col1=?')
    self.assertEqual(args, [1,2,3])

  def test_nested_conditional2(self):
    sql, args = Something.ALL.where((Something.col1==3) | (Something.col1==1) & (Something.col2==2))._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=? or (col1=? and col2=?)')
    self.assertEqual(args, [1,2,3])




if __name__ == '__main__':
    unittest.main()


