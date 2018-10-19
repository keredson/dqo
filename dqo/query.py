import asyncio

class Query(object):
  
  def __init__(self, cls):
    self._cls = cls
    
  def __iter__(self):
    return [].__iter__()
  
  def __aiter__(self):
    return AsyncIterable()
  
  def select(self, *columns):
    pass
  
  def plus(self, *fks):
    pass
    
  def bind(self, db_or_tx):
    pass
  
  def first(self):
    if asyncio.get_running_loop():
      return self.__aiter__().__anext__()
    return 1
      
  def __len__(self):
    if asyncio.get_running_loop():
      return 1
    return 1
      
  
    


class AsyncIterable:
  async def __anext__(self):
    if hasattr(self,'done'):
      raise StopAsyncIteration
    self.done = True
    return 1

