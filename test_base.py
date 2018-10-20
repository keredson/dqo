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


class BaseDB:

  def setUp(self):
    Something.ALL.delete()

  def test_insert_and_select_all(self):
    Something.ALL.insert(col1=1)
    saw_something = False
    for something in Something.ALL:
      self.assertEqual(something.col1, 1)
      saw_something = True
    self.assertTrue(saw_something)

  def test_save_and_select_all(self):
    something = Something()
    something.col1 = 1
    something.save()
    saw_something = False
    for something in Something.ALL:
      self.assertEqual(something.col1, 1)
      saw_something = True
    self.assertTrue(saw_something)

  def test_select_nothing(self):
    self.assertEqual(list(Something.ALL), [])

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



