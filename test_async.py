import asyncio

import dqo

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


@dqo.table
class Something:
  col1 = dqo.Column(int)
  col2 = dqo.Column(str)


class BaseAsync:

  @async_test
  async def setUp(self):
    await Something.ALL.delete()

  @async_test
  async def test_select_all_async(self):
    await Something.ALL.insert(col1=1)
    saw_something = False
    async for something in Something.ALL:
      self.assertEqual(something.col1, 1)
      saw_something = True
    self.assertTrue(saw_something)

  @async_test
  async def test_first_async(self):
    self.assertEqual(await Something.ALL.first(), None)

  @async_test
  async def test_len_async(self):
    self.assertEqual(len(Something.ALL), 1)


