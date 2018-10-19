import asyncio, copy, enum, io

from .column import Column, AlsoSelect, UnSelect, Condition

class CMD(enum.Enum):
  SELECT = 1
  INSERT = 2
  UPDATE = 3
  DELETE = 4

class Query(object):
  
  def __init__(self, tbl):
    self._tbl = tbl
    self._db_ = None
    self._cmd = CMD.SELECT
    self._select = None
    self._conditions = []
    
  def bind(self, db):
    self._db_ = db
    
  def __iter__(self):
    return [].__iter__()
  
  def __aiter__(self):
    return AsyncIterable()
  
  def select(self, *columns):
    self = copy.deepcopy(self)
    if self._select is None:
      self._select = self._tbl._columns
    existing = set([c._db_name for c in self._select])
    select = []
    unselect = set()
    alsoselect = []
    for x in columns:
      if isinstance(x, Column):
        select.append(x)
      elif isinstance(x, AlsoSelect):
        alsoselect.append(x.column)
      elif isinstance(x, UnSelect):
        unselect.add(x.column._db_name)
      else:
        raise ValueError('unknown type %s' % s)
    if select and (unselect or alsoselect):
      raise ValueError('You must either select a set of columns (MyTable.col) or a set of selection modifiers (+MyTable.col / -MyTable.col), you cannot mix the two.')
    if unselect:
      self._select = [c for c in self._select if c._db_name not in unselect]
    elif alsoselect:
      self._select += [c for c in alsoselect if c._db_name not in existing]
    else:
      self._select = select
    return self
  
  def where(self, *conditions, **kwargs):
    conditions = list(conditions)
    self = copy.deepcopy(self)
    for name, value in kwargs.items():
      conditions.append(getattr(self._tbl, name)==value)
    self._conditions += conditions
    return self
    
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

  @property
  def _db(self):
    if self._db_: return self._db_
    if self._tbl._db: return self._tbl._db
    return dqo.DEFAULT_DB
      
  def _sql(self):
    dialect = self._db.dialect
    sql = io.StringIO()
    args = []
    self._sql_(dialect, sql, args)
    return sql.getvalue(), args
  
  def _sql_(self, d, sql, args):
    if self._cmd==CMD.SELECT:
      self._select_sql_(d, sql, args)
    if self._cmd==CMD.INSERT:
      self._insert_sql_(d, sql, args)
    if self._cmd==CMD.UPDATE:
      self._select_sql_(d, sql, args)
    if self._cmd==CMD.DELETE:
      self._select_sql_(d, sql, args)

  def _select_sql_(self, d, sql, args):
    sql.write('select ')
    sql.write(self._gen_select(d))
    sql.write(' from ')
    sql.write(d.term(self._tbl._name))
    self._gen_where(d, sql, args)
      
      
  def _gen_select(self, d):
    columns = self._tbl._columns if self._select is None else self._select
    return ','.join([d.term(c._db_name) for c in columns])
    
  def _gen_where(self, d, sql, args):
    if not self._conditions: return
    sql.write(' where ')
    condition = self._conditions[0] if len(self._conditions)==1 else Condition('and', self._conditions)
    condition._sql_(d, sql, args)
    


class AsyncIterable:
  async def __anext__(self):
    if hasattr(self,'done'):
      raise StopAsyncIteration
    self.done = True
    return 1


import dqo
