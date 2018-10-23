import asyncio, inspect, re

from .query import Query
from .column import Column
  

class BaseRow(object):

  def __init__(self, **kwargs):
    kwargs = {k:v for k,v in kwargs.items() if not k.startswith('_')}
    self.__dict__.update(kwargs)
    self.__dict__['_new'] = True
    self.__dict__['_dirty'] = set(kwargs.keys())

  def __setattr__(self, attr, value):
    dirty = not attr.startswith('_') and (attr not in self.__dict__ or self.__dict__[attr]!=value)
    self.__dict__[attr] = value
    if dirty:
      self.__dict__['_dirty'].add(attr)
    return value

  def save(self):
    if self._new: self.insert()
    else: self.update()
  
  def insert(self):
    if asyncio.get_running_loop():
      async def f():
        await self._tbl.ALL.insert(**self.__dict__)
        self.__dict__['_new'] = False
        self.__dict__['_dirty'] = set()
        return self
      return f()
    else:
      self._tbl.ALL.insert(**self.__dict__)
      self.__dict__['_new'] = False
      self.__dict__['_dirty'] = set()
      return self
  
  def update(self):
    if not self._tbl._pk: raise Exception("cannot update a row without a primary key")
    self._tbl.ALL.set(**{x:self.__dict__.get(x) for x in self._dirty}).where(*[c==self.__dict__.get(c._name) for c in self._tbl._pk]).update()
    self.__dict__['_dirty'] = set()
  
  def delete(self):
    if not self._tbl._pk: raise Exception("cannot delete a row without a primary key")
    self._tbl.ALL.where(*[c==self.__dict__.get(c._name) for c in self._tbl._pk]).delete()
    self.__dict__['_new'] = True
    self.__dict__['_dirty'] = set()
  
  def __repr__(self):
    return '<%s %s>' % (self.__class__.__name__, ' '.join(['%s=%s' % (k,repr(v)) for k,v in self.__dict__.items() if not k.startswith('_')]))


def TableDecorator():
  def f(cls, name=None, sync_db=None, async_db=None):
    return build_table(cls, name=name, sync_db=sync_db, async_db=async_db)
  return f

  
def build_table(cls, name=None, sync_db=None, async_db=None):

  Table = cls

  def __new__(cls, **kwargs):
    return Row(**kwargs)
  Table.__new__ = __new__
  def __instancecheck__(self, instance):
    return isinstance(instance, Row)
  Table.__instancecheck__ = __instancecheck__

  class Row(BaseRow):
    _tbl = Table
  
  Table._dqoi_db_name = cc_to_snake(cls.__name__)
  Table._dqoi_columns = get_columns(cls)
  Table.ALL = Query(Table)
  Table.__name__ = cls.__name__
  Row.__name__ = cls.__name__
  for col in Table._dqoi_columns:
    setattr(Table, col._name, col)
  Table._pk = tuple([c for c in Table._dqoi_columns if c.primary_key])
  Table._db = cls._db if hasattr(cls, '_db') else None
  
  return Table


def get_columns(cls):
  ret = []
  for name, value in inspect.getmembers(cls):
    if not isinstance(value, Column): continue
    value._set_name(name)
    ret.append(value)
  return ret

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
def cc_to_snake(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()

