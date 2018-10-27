import asyncio, inspect, re

from .query import Query
from .column import Column, PrimaryKey, ForeignKey, Index
  
  
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
  
  def __dqoi_save_pk(self, pk):
    if pk is None: return
    if len(self._tbl._dqoi_pk.columns) == 1:
      self.__dict__[self._tbl._dqoi_pk.columns[0].name] = pk
    else:
      for c,v in zip(self._tbl._dqoi_pk.columns, pk):
        self.__dict__[c.name] = v
  
  def insert(self):
    if asyncio.get_running_loop():
      async def f():
        pk = await self._tbl.ALL.insert(**self.__dict__)
        self.__dqoi_save_pk(pk)
        self.__dict__['_new'] = False
        self.__dict__['_dirty'] = set()
        return self
      return f()
    else:
      pk = self._tbl.ALL.insert(**self.__dict__)
      self.__dqoi_save_pk(pk)
      self.__dict__['_new'] = False
      self.__dict__['_dirty'] = set()
      return self
  
  def update(self):
    if not self._tbl._dqoi_pk: raise Exception("cannot update a row without a primary key")
    q = self._tbl.ALL.set(**{x:self.__dict__.get(x) for x in self._dirty}).where(*[c==self.__dict__.get(c.name) for c in self._tbl._dqoi_pk.columns])
    if asyncio.get_running_loop():
      async def f():
        await q.update()
        self.__dict__['_dirty'] = set()
      return f()
    else:
      q.update()
      self.__dict__['_dirty'] = set()
  
  def delete(self):
    if not self._tbl._dqoi_pk: raise Exception("cannot delete a row without a primary key")
    if asyncio.get_running_loop():
      async def f():
        await self._tbl.ALL.where(*[c==self.__dict__.get(c.name) for c in self._tbl._dqoi_pk.columns]).delete()
        self.__dict__['_new'] = True
        self.__dict__['_dirty'] = set()
      return f()
    else:
      self._tbl.ALL.where(*[c==self.__dict__.get(c.name) for c in self._tbl._dqoi_pk.columns]).delete()
      self.__dict__['_new'] = True
      self.__dict__['_dirty'] = set()
  
  def __repr__(self):
    return '<%s %s>' % (self.__class__.__name__, ' '.join(['%s=%s' % (k,repr(v)) for k,v in self.__dict__.items() if not k.startswith('_')]))


def TableDecorator(name=None, db=None, aka=None):
  '''
  :param name: The name of the table in the database.
  :param db: The database to use for regular Python code.  If ``None`` defaults to ``dqo.DB`` if defined.
  :param aka: A string or list of strings with previous names of this table, used for renaming.
  
  A decorator used to turn a ``class`` into a dqo database table.  For example:
  
  .. code-block:: python
  
    @dqo.Table()
    class Product:
      name = dqo.Column(str)
      keywords = dqo.Column([str])
  '''
  def f(cls):
    return build_table(cls, name=name, db=db, aka=aka)
  return f


class AliasedTable:
  def __init__(self, tbl, name):
    self.tbl = tbl
    self.name = name
    self._dqoi_db = tbl._dqoi_db

  def _sql_(self, d, sql, args):
    if hasattr(self.tbl,'_dqoi_db_name'):
      sql.write(d.term(self.tbl._dqoi_db_name))
    else:
      self.tbl._sql_(d, sql, args)
    sql.write(' as ')
    sql.write(d.term(self.name))

  
def build_table(cls, name=None, db=None, aka=None):

  if aka is None: aka = set()
  elif isinstance(aka,str): aka = set([aka])
  else: aka = set(aka)

  def __new__(cls, **kwargs):
    return Row(**kwargs)
  cls.__new__ = __new__
  def __instancecheck__(self, instance):
    return isinstance(instance, Row)
  cls.__instancecheck__ = __instancecheck__
  cls.as_ = lambda name: AliasedTable(cls, name)
  def _sql_(d, sql, args):
    sql.write(d.term(cls._dqoi_db_name))
    sql.write(' as ')
    sql.write(d.term(d.registered[cls]))    
  cls._sql_ = _sql_

  class Row(BaseRow):
    _tbl = cls
  
  cls._dqoi_db_name = name or cc_to_snake(cls.__name__)
  cls._dqoi_db = db
  cls._dqoi_pks = [x for x in cls.__dict__.values() if isinstance(x,PrimaryKey)]
  cls._dqoi_indexes = [x for x in cls.__dict__.values() if isinstance(x,Index)]
  cls._dqoi_columns = get_columns(cls)
  cls._dqoi_aka = aka
  cls._dqoi_fks = get_fks(cls)
  cls._dqoi_columns_by_attr_name = {c._name:c for c in cls._dqoi_columns}
    
  cls.ALL = Query(cls)
  cls.__name__ = cls.__name__
  Row.__name__ = cls.__name__
  
  if len(cls._dqoi_pks)>1:
    raise ValueError('there can be only one (primary key): %s' % cls._dqoi_pks)
  cls._dqoi_pk = cls._dqoi_pks[0] if cls._dqoi_pks else None
  del cls._dqoi_pks
  
  if db:
    db._known_tables.append(cls)
  
  return cls


def get_columns(cls):
  ret = []
  for name, value in cls.__dict__.items():
    if name.startswith('__'): continue 
    if not isinstance(value, Column): continue
    value._set_name(name)
    value.tbl = cls
    ret.append(value)
    if value.primary_key:
      cls._dqoi_pks.append(PrimaryKey(value))
    if value.index or value.unique:
      cls._dqoi_indexes.append(Index(value, unique=value.unique))
  return ret


def get_fks(cls):
  ret = []
  for name, value in list(cls.__dict__.items()):
    if name.startswith('__'): continue
    if not isinstance(value, ForeignKey): continue
    value._name = name
    value.tbl = cls
    ret.append(value)
    value._gen_columns()
  return ret



first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
def cc_to_snake(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()

