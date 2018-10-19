import unittest
import asyncio

import dqo

from test_models import *

def async_test(f):
  event_loop = asyncio.new_event_loop()
  asyncio.set_event_loop(event_loop) # needed?
  def test_f(self):
    def f2():
      return f(self)
    coro = asyncio.coroutine(f2)
    event_loop.run_until_complete(coro())
    event_loop.close()
  test_f.__name__ = f.__name__
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

if __name__ == '__main__':
    unittest.main()

