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


@dqo.Table
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
  async def test_order_by(self):
    await Something.ALL.insert(col1=1)
    await Something.ALL.insert(col1=2)
    o = await Something.ALL.order_by(Something.col1).first()
    self.assertEqual(o.col1, 1)
    o = await Something.ALL.order_by(Something.col1.desc).first()
    self.assertEqual(o.col1, 2)

  @async_test
  async def test_count(self):
    await Something.ALL.insert(col1=1)
    await Something.ALL.insert(col1=2)
    self.assertEqual(await Something.ALL.count(), 2)
    
  @async_test
  async def test_count_by(self):
    await Something.ALL.insert(col1=1)
    await Something.ALL.insert(col1=2)
    self.assertEqual(await Something.ALL.count_by(Something.col1), {1:1,2:1})
    
  @async_test
  async def test_count_by_order(self):
    await Something.ALL.insert(col1=1)
    await Something.ALL.insert(col1=2)
    d = await Something.ALL.order_by(dqo.sql.count.desc).count_by(Something.col1)
    self.assertEqual(list(d.items()), [(2,1),(1,1)])

  @async_test
  async def test_count_by_2_cols(self):
    await Something.ALL.insert(col1=1)
    await Something.ALL.insert(col1=2)
    self.assertEqual(await Something.ALL.count_by(Something.col1, Something.col2), {(1,None):1,(2,None):1})

