import asyncio, copy, enum, io

from .column import Column, PosColumn, NegColumn, Condition
from .database import Dialect
from .connection import TLS
from .function import sql

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
    self._order_by = None
    self._group_by = None
  
  def __copy__(self):
    new = Query(self._tbl)
    new._db_ = self._db_
    new._cmd = self._cmd
    new._select = copy.copy(self._select)
    new._set_values = copy.copy(self._set_values)
    new._conditions = copy.copy(self._conditions)
    new._limit = self._limit
    new._order_by = copy.copy(self._order_by)
    new._group_by = copy.copy(self._group_by)
    return new
    
  def __iter__(self):
    if self._select is None:
      self = copy. copy(self)
      self._select = self._tbl._columns
    return SyncIterable(self)
  
  def __aiter__(self):
    if self._select is None:
      self = copy. copy(self)
      self._select = self._tbl._columns
    return AsyncIterable(self)
  
  def select(self, *columns):
    self = copy. copy(self)
    if self._select is None:
      self._select = self._tbl._columns
    existing = set([c._db_name for c in self._select])
    select = []
    unselect = set()
    alsoselect = []
    for x in columns:
      if isinstance(x, Column):
        select.append(x)
      elif isinstance(x, PosColumn):
        alsoselect.append(x.column)
      elif isinstance(x, NegColumn):
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
    self = copy. copy(self)
    for name, value in kwargs.items():
      conditions.append(getattr(self._tbl, name)==value)
    self._conditions += conditions
    return self
    
  def plus(self, *fks):
    # TODO
    pass
    
  def order_by(self, *columns):
    self = copy. copy(self)
    self._order_by = columns
    return self
    
  def bind(self, db_or_tx):
    self = copy. copy(self)
    self._db_ = db_or_tx
    return self
  
  def first(self):
    self = self.limit(1)
    if self._select is None:	
      self = copy. copy(self)	
      self._select = self._tbl._columns	
    if asyncio.get_running_loop():
      sql, args = self._sql()
      keys = [c._name for c in self._select]
      async def f():
        async with self._db.connection as conn:
          data = await conn.async_fetch(sql, args)
          if data:
            row = data[0]
            o = self._tbl()
            o.__dict__.update(dict(zip(keys, row)))
            return o
          else:
            return None
      return f()
    else:
      values = list(self)
      return values[0] if len(values) else None

  def limit(self, n):
    self = copy. copy(self)
    self._limit = n
    return self

  def top(self, n):
    return self.limit(n)

  def set(self, **kwargs):
    self = copy. copy(self)
    self._set_values.update(kwargs)
    return self
      
  def delete(self):
    self = copy. copy(self)
    self._cmd = CMD.DELETE
    return self._execute()
  
  def count(self):
    self = copy. copy(self)
    self._select = [sql.count(sql(1))]
    return self._fetch_scalar()

  def count_by(self, *columns):
    self = copy. copy(self)
    self._select = list(columns) + [sql.count(sql(1))]
    self._group_by = columns
    return self._fetch_map(len(columns))

  def _fetch_map(self, len_keys):
    sql, args = self._sql()
    if asyncio.get_running_loop():
      return self._async_fetch_map(sql, args, len_keys)
    else: 
      return self._sync_fetch_map(sql, args, len_keys)

  @property
  def _conn_or_tx_sync(self):
    conn_or_tx = TLS.conn_or_tx if hasattr(TLS,'conn_or_tx') else None
    if not conn_or_tx:
      conn_or_tx = self._db.connection
    return conn_or_tx

  @property
  def _conn_or_tx_async(self):
    return self._db.connection

  def _sync_fetch_map(self, sql, args, len_keys):
    with self._conn_or_tx_sync as conn:
      data = list(conn.sync_fetch(sql, args))
      if len_keys > 1:
        data = [(tuple(r[:len_keys]),r[len_keys]) for r in data]
      return dict(data)
        
  async def _async_fetch_map(self, sql, args, len_keys):
    async with self._conn_or_tx_async as conn:
      data = await conn.async_fetch(sql, args)
      if len_keys > 1:
        data = [(tuple(r[:len_keys]),r[len_keys]) for r in data]
      return dict(data)
      
  def _fetch_scalar(self):
    sql, args = self._sql()
    if asyncio.get_running_loop():
      return self._async_fetch_scalar(sql, args)
    else: 
      return self._sync_fetch_scalar(sql, args)

  def _sync_fetch_scalar(self, sql, args):
    with self._conn_or_tx_sync as conn:
      data = list(conn.sync_fetch(sql, args))
      return data[0][0] if data and data[0] else None
        
  async def _async_fetch_scalar(self, sql, args):
    async with self._conn_or_tx_async as conn:
      data = await conn.async_fetch(sql, args)
      return data[0][0] if data and data[0] else None
      
  def update(self):
    self = copy. copy(self)
    self._cmd = CMD.UPDATE
    return self._execute()
  
  def insert(self, *instances, **data):
    self = copy. copy(self)
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
    with self._conn_or_tx_sync as conn:
      return conn.sync_execute(sql, args)
        
  async def _async_execute(self, sql, args):
    async with self._conn_or_tx_async as conn:
      return await conn.async_execute(sql, args)
    
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
    dialect = (db.dialect if db else Dialect.GENERIC).for_query()
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
    self._gen_select(d, sql, args)
    sql.write(' from ')
    sql.write(d.term(self._tbl._name))
    self._gen_where(d, sql, args)
    if self._group_by:
      sql.write(' group by ')
      first = True
      for col in self._group_by:
        if first: first = False
        else: sql.write(',')
        col._sql_(d, sql, args)
    if self._order_by:
      sql.write(' order by ')
      first = True
      for col in self._order_by:
        if first: first = False
        else: sql.write(', ')
        col._sql_(d, sql, args)
    if self._limit is not None:
      sql.write(' limit ')
      args.append(self._limit)
      sql.write(d.arg)
      
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
      sql.write(d.arg)
    self._gen_where(d, sql, args)
      
  def _insert_sql_(self, d, sql, args):
    sql.write('insert into ')
    sql.write(d.term(self._tbl._name))
    sql.write(' (')
    values = []
    first = True
    for k,v in self._insert.items():
      if k.startswith('_'): continue
      if first: first = False
      else: sql.write(',')
      sql.write(d.term(k))
      args.append(v)
      values.append(d.arg)
    sql.write(') values (')
    sql.write(','.join(values))
    sql.write(')')
      
  def _delete_sql_(self, d, sql, args):
    sql.write('delete from ')
    sql.write(d.term(self._tbl._name))
    self._gen_where(d, sql, args)
            
  def _gen_select(self, d, sql, args):
    components = self._tbl._columns if self._select is None else self._select
    first = True
    for component in components:
      if first: first = None
      else: sql.write(',')
      if hasattr(component,'_sql_'):
        component._sql_(d, sql, args)
      else:
        sql.write(d.arg)
        args.append(component)
    
  def _gen_where(self, d, sql, args):
    if not self._conditions: return
    sql.write(' where ')
    condition = self._conditions[0] if len(self._conditions)==1 else Condition('and', self._conditions)
    condition._sql_(d, sql, args)


class AsyncIterable:

  def __init__(self, query):
    self.query = query
    sql, args = query._sql()
    self.keys = [c._name for c in query._select]
    self._inited = False
  
  async def _init(self):
    sql, args = self.query._sql()
    async with self.query._db.connection as conn:
      data = await conn.async_fetch(sql, args)
      async def f():
        for x in data:
          yield x
      self.iter = f().__aiter__()
      self.data = data
      self.iter = data.__iter__()
    self._inited = True

  async def __anext__(self):
    if not self._inited: await self._init()
    try:
      row = self.iter.__next__()
    except StopIteration:
      raise StopAsyncIteration
    o = self.query._tbl()
    o.__dict__.update(dict(zip(self.keys, row)))
    return o


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
