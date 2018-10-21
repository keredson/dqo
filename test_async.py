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
  async def test_select_all(self):
    await Something.ALL.insert(col1=1)
    saw_something = False
    async for something in Something.ALL:
      self.assertEqual(something.col1, 1)
      saw_something = True
    self.assertTrue(saw_something)

  @async_test
  async def test_first_none(self):
    self.assertEqual(await Something.ALL.first(), None)

  @async_test
  async def test_first_something(self):
    await Something.ALL.insert(col1=1)
    self.assertEqual((await Something.ALL.first()).col1, 1)

  @async_test
  async def test_len_async(self):
    self.assertEqual(len(Something.ALL), 1)

  @async_test
  async def test_order_by(self):
    await Something.ALL.insert(col1=1)
    await Something.ALL.insert(col1=2)
    o = await Something.ALL.order_by(Something.col1).first()
    self.assertEqual(o.col1, 1)
    o = await Something.ALL.order_by(Something.col1.desc).first()
    self.assertEqual(o.col1, 2)

