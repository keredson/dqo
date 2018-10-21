import asyncio, copy, enum, io

from .column import Column, AlsoSelect, UnSelect, Condition
from .database import Dialect
from .connection import TLS

class CMD(enum.Enum):
  SELECT = 1
  INSERT = 2
  UPDATE = 3
  DELETE = 4
  INSERT_MANY = 5

class Query(object):
  
  def __init__(self, tbl):
    self._tbl = tbl
    self._db_ = None
    self._cmd = CMD.SELECT
    self._select = None
    self._set_values = {}
    self._conditions = []
    self._limit = None
    
  def __iter__(self):
    if self._select is None:
      self = copy.deepcopy(self)
      self._select = self._tbl._columns
    return SyncIterable(self)
  
  def __aiter__(self):
    if self._select is None:
      self = copy.deepcopy(self)
      self._select = self._tbl._columns
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
    # TODO
    pass
    
  def bind(self, db_or_tx):
    self = copy.deepcopy(self)
    self._db_ = db_or_tx
    return self
  
  def first(self):
    self = self.limit(1)
    if asyncio.get_running_loop():
      return self.__aiter__().__anext__()
    else:
      values = list(self)
      return values[0] if len(values) else None

  def limit(self, n):
    self = copy.deepcopy(self)
    self._limit = n
    return self

  def top(self, n):
    return self.limit(n)

  def set(self, **kwargs):
    self = copy.deepcopy(self)
    self._set_values.update(kwargs)
    return self
      
  def delete(self):
    self = copy.deepcopy(self)
    self._cmd = CMD.DELETE
    return self._execute()
  
  def update(self):
    self = copy.deepcopy(self)
    self._cmd = CMD.UPDATE
    return self._execute()
  
  def insert(self, *instances, **data):
    self = copy.deepcopy(self)
    if instances and data: raise ValueError('Please pass in either args or kwargs, not both.')
    if instances:
      if len(instances)==1:
        self._cmd = CMD.INSERT
        self._insert = instances[0]
      else:
        self._cmd = CMD.INSERT_MANY
        self._insert = instances
    elif data:
      self._cmd = CMD.INSERT
      self._insert = data
    return self._execute()
  
  def _execute(self):
    sql, args = self._sql()
    if asyncio.get_running_loop():
      return self._async_execute(sql, args)
    else: 
      return self._sync_execute(sql, args)
  
  def _sync_execute(self, sql, args):
    conn_or_tx = TLS.conn_or_tx if hasattr(TLS,'conn_or_tx') else None
    with conn_or_tx or self._db.connection as conn:
      return conn.sync_execute(sql, args)
        
  async def _async_execute(self, sql, args):
    async with self._db.connection as conn:
      return await conn.async_execute(sql, args)
    
      
  def __len__(self):
    # TODO
    if asyncio.get_running_loop():
      return 1
    return 1

  @property
  def _db(self):
    if self._db_: return self._db_
    if self._tbl._db: return self._tbl._db
    if asyncio.get_running_loop():
      return dqo.DEFAULT_ASYNC_DB
    else:
      return dqo.DEFAULT_SYNC_DB
      
  def _sql(self):
    db = self._db
    dialect = db.dialect if db else Dialect.GENERIC
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
      self._update_sql_(d, sql, args)
    if self._cmd==CMD.DELETE:
      self._delete_sql_(d, sql, args)

  def _select_sql_(self, d, sql, args):
    sql.write('select ')
    sql.write(self._gen_select(d))
    sql.write(' from ')
    sql.write(d.term(self._tbl._name))
    self._gen_where(d, sql, args)
    if self._limit is not None:
      sql.write(' limit ')
      args.append(self._limit)
      sql.write(d.arg(len(args)))
      
  def _update_sql_(self, d, sql, args):
    sql.write('update ')
    sql.write(d.term(self._tbl._name))
    sql.write(' set ')
    first = True
    for k,v in self._set_values.items():
      if first: first = False
      else: sql.write(', ')
      sql.write(d.term(k))
      sql.write('=')
      args.append(v)
      sql.write(d.arg(len(args)))
    self._gen_where(d, sql, args)
      
  def _insert_sql_(self, d, sql, args):
    sql.write('insert into ')
    sql.write(d.term(self._tbl._name))
    sql.write(' (')
    c = len(args)
    values = []
    first = True
    for k,v in self._insert.items():
      if k.startswith('_'): continue
      if first: first = False
      else: sql.write(',')
      sql.write(d.term(k))
      args.append(v)
      c += 1
      values.append(d.arg(c))
    sql.write(') values (')
    sql.write(','.join(values))
    sql.write(')')
      
  def _delete_sql_(self, d, sql, args):
    sql.write('delete from ')
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


class SyncIterable:
  def __init__(self, query):
    self.query = query
    sql, args = query._sql()
    self.keys = [c._name for c in query._select]
    conn_or_tx = TLS.conn_or_tx if hasattr(TLS,'conn_or_tx') else None
    if conn_or_tx:
      self.iter = conn_or_tx.sync_fetch(sql, args).__iter__()
    else:
      with query._db.connection as conn:
        self.iter = list(conn.sync_fetch(sql, args)).__iter__()

  def __next__(self):
    row = self.iter.__next__()
    o = self.query._tbl()
    o.__dict__.update(dict(zip(self.keys, row)))
    return o

import dqo
