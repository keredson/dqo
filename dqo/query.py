import asyncio, copy, enum, io

from .column import Column, PosColumn, NegColumn, Condition, InnerQuery
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
    self._joins = []
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
    new._joins = copy.copy(self._joins)
    new._conditions = copy.copy(self._conditions)
    new._limit = self._limit
    new._order_by = copy.copy(self._order_by)
    new._group_by = copy.copy(self._group_by)
    return new
    
  def __iter__(self):
    '''
    All queries are iterable.  For example:
    
    .. code-block:: python
    
      for user in User.ALL:
        # do something
    
    If in a transaction or a with block defining the scope of the connection, the results will stream.  If a query
    has to open its own connection it will load all records before streaming.  This is because there is no 
    guarantee an iterator will complete, and waiting for the garbage collector is a fool's game.
    '''
    if self._select is None:
      self = copy. copy(self)
      self._select = self._tbl._dqoi_columns
    return SyncIterable(self)
  
  def __aiter__(self):
    '''
    All queries are async iterable.  For example:
    
    .. code-block:: python
    
      async for user in User.ALL:
        # do something asynchronously
    '''
    if self._select is None:
      self = copy. copy(self)
      self._select = self._tbl._dqoi_columns
    return AsyncIterable(self)
  
  def select(self, *columns):
    '''
    :param columns: One or more columns to select.

    By default all columns on a table are selected.  You can customize this by calling ``select()`` with one
    or more columns which should be selected in the query.  For instance, to select *only*:
    
    .. code-block:: python
    
      User.ALL.select(User.id, User.name, User.email)
      
    To select all columns *except* the user's email, prepend the negative operator:
    
    .. code-block:: python
    
      User.ALL.select(-User.email)
      
    To add a column (or something else, like a function) for selection, use the positive operator:
        
    .. code-block:: python
    
      User.ALL.select(+dqo.sql.LOWER(User.first_name))
      
    If you send in an explicit list (no +/-) you will replace the existing selected columns.  If you pass in +/- columns,
    you will modify the existing selected columns.  You cannot do both in the came call.
    '''
    self = copy.copy(self)
    if self._select is None:
      self._select = self._tbl._dqoi_columns
    existing = set([c.name for c in self._select])
    select = []
    unselect = set()
    alsoselect = []
    for x in columns:
      if isinstance(x, Column):
        select.append(x)
      elif isinstance(x, PosColumn):
        alsoselect.append(x.column)
      elif isinstance(x, NegColumn):
        unselect.add(x.column.name)
      else:
        raise ValueError('unknown type %s' % s)
    if select and (unselect or alsoselect):
      raise ValueError('You must either select a set of columns (MyTable.col) or a set of selection modifiers (+MyTable.col / -MyTable.col), you cannot mix the two.')
    if unselect:
      self._select = [c for c in self._select if c.name not in unselect]
    elif alsoselect:
      self._select += [c for c in alsoselect if c.name not in existing]
    else:
      self._select = select
    return self
  
  def where(self, *conditions, **kwargs):
    '''
    Adds conditions to a query.  For example:
    
    .. code-block:: python
    
      User.ALL.where(User.email == 'someone@somewhere.com')
      
    Keyword arguments will be evaluated to fields on the bound table.  For example, the above statement could also be written as:

    .. code-block:: python
    
      User.ALL.where(email='someone@somewhere.com')
    
    Multiple calls to ``where()`` will result in a SQL ``AND``.  For example:
      
    .. code-block:: python
    
      User.ALL.where(name='John').where(email='me@here.com')
      
    Would result in:
      
    .. code-block:: sql
    
      select * from users
      where name='John' and email='me@here.com'
      
    All of the following would also result in the same SQL as above:
    
    .. code-block:: python
    
      User.ALL.where(name='John', email='me@here.com')
      User.ALL.where(User.name=='John', User.email=='me@here.com')
      User.ALL.where(
        (User.name=='John') & (User.email=='me@here.com')
      )
      
    An ``OR`` statment can be created with the bitwise or operator:
    
    .. code-block:: python
    
      User.ALL.where(
        (User.name=='John') | (User.email=='me@here.com')
      )
      
    When using the ``&`` and ``|`` operators, make sure you wrap the condition in parentheses as they have
    lower precedence than others like ``==``.
    '''
    conditions = list(conditions)
    self = copy. copy(self)
    for name, value in kwargs.items():
      conditions.append(getattr(self._tbl, name)==value)
    self._conditions += conditions
    return self
    
  def plus(self, *fks):
    # TODO
    pass
    
  def left_join(self, other, on=None):
    '''
    Performs a left join.
    '''
    self = copy.copy(self)
    self._joins.append(Join('left', other, on))
    return self
    
  def right_join(self, other, on=None):
    '''
    Performs a right join.
    '''
    self = copy.copy(self)
    self._joins.append(Join('right', other, on))
    return self
    
  def inner_join(self, other, on=None):
    '''
    Performs an inner join.
    '''
    self = copy.copy(self)
    self._joins.append(Join('inner', other, on))
    return self
    
  def full_outer_join(self, other, on=None):
    '''
    Performs a full outer join.
    '''
    self = copy.copy(self)
    self._joins.append(Join('full outer', other, on))
    return self
    
  def order_by(self, *columns):
    '''
    Specifies the ordering for the query.  Example:
    
    .. code-block:: python
      
      User.ALL.order_by(User.last_name, User.first_name)
      
    To control the order:
    
    .. code-block:: python
      
      User.ALL.order_by(User.name.asc)
      User.ALL.order_by(User.name.desc)
      
    Every call clobbers any previous calls to ``order_by``.
    
    Only effects ``SELECT`` queries.  Other query types accept but ignore order by directives.  They intentionally don't 
    throw an error, as it's common to change a select into something else as part of a workflow.  Example:
    
    .. code-block:: python
      
      to_delete = User.ALL.where(name='John').order_by(User.kind)
      preview(to_delete) # display who's to be deleted
      to_delete.delete()
    '''
    self = copy. copy(self)
    self._order_by = columns
    return self
    
  def bind(self, db_or_tx):
    '''
    :param db_or_tx: Either a database or a transaction object.
    
    Specifies what database or connection transaction to use for this query.  For example:
    
    .. code-block:: python
    
      db = dqo.Database(src=...)
      q = User.ALL.where(...).bind(db)
      for user in q:
        # do something
    '''
    self = copy. copy(self)
    self._db_ = db_or_tx
    return self
  
  def first(self):
    '''
    :returns: An instance of the selected type or ``None`` if not found.
    
    Adds a ``.limit(1)`` to a given query and returns the first result (if any).  Example:
    
    .. code-block:: python
    
      user = User.ALL.first()

    In async code:
    
    .. code-block:: python
    
      user = await User.ALL.first()
    '''
    self = self.limit(1)
    if self._select is None:	
      self = copy. copy(self)	
      self._select = self._tbl._dqoi_columns	
    if asyncio.get_running_loop():
      sql, args = self._sql()
      keys = [c._name for c in self._select]
      async def f():
        async with self._db.connection() as conn:
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
    '''
    :param int: The max number of rows to return.
    
    Example:
    
    .. code-block:: python
    
      User.ALL.limit(10)
    
    Equivalent to ``.top(10)``. 
    '''
    self = copy. copy(self)
    self._limit = n
    return self

  def top(self, n):
    return self.limit(n)

  def set(self, **kwargs):
    '''
    Sets values in preperation for an update.  Example:
    
    .. code-block:: python
    
      User.ALL.where(id=1).set(name='John').update()
        
  .. py:method:: top(int)
  
    :param int: The max number of rows to return.
    
    Example:
    
    .. code-block:: python
    
      User.ALL.top(10)
    
    Equivalent to ``.limit(10)``. 
    '''
    self = copy. copy(self)
    self._set_values.update(kwargs)
    return self
      
  def delete(self):
    '''
    :returns: The number of rows deleted.
    
    Deletes the records matched by a given query.  Example:
    
    .. code-block:: python
    
      >>> User.ALL.where(id=1).delete()
      1
    '''
    self = copy. copy(self)
    self._cmd = CMD.DELETE
    return self._execute()
  
  def count(self):
    '''
    :returns: The number of rows matching the query.
    
    does a ``count(1)`` of the existing query.  Example:
    
    .. code-block:: python
    
      >>> User.ALL.count()
      42
    '''
    self = copy. copy(self)
    self._select = [sql.count(sql(1))]
    return self._fetch_scalar()

  def count_by(self, *columns):
    '''
    :returns: A ``dict`` where the keys are the db values of the columns selected and the values are their counts.
    
    .. code-block:: python
    
      >>> User.ALL.count_by(User.name)
      {'John':1, 'Paul':2}
      
    If multiple columns are passed, the keys will be tuples.

    .. code-block:: python
    
      >>> User.ALL.count_by(User.first_name, User.last_name)
      {('John','Smith'):1, ('Paul','Anderson'):2}
    '''
    self = copy.copy(self)
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
      conn_or_tx = self._db.connection()
    return conn_or_tx

  @property
  def _conn_or_tx_async(self):
    return self._db.connection()

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
    '''
    :returns: The number of rows updated.
    
    Executes an update for previously set values.
    
    .. code-block:: python
    
      User.ALL.where(id=1).set(name='John').update()
    '''
    self = copy. copy(self)
    self._cmd = CMD.UPDATE
    return self._execute()
  
  def insert(self, *args, **data):
    '''
    Inserts one or more rows.  If keyword arguments are passed, a single row is inserted returning the primary key (if defined).  For example:

    .. code-block:: python
      
      >>> User.ALL.insert(name='John', email='me@here.org')
      42
      
    If you have a dict of values:

    .. code-block:: python
      
      user = User.ALL.insert(**{'name':'John', 'email':'me@here.org'})
    
    If a list of ``dicts`` are passed, multiple rows are inserted efficiently in a single query, and a list of their primary keys are returned:

    .. code-block:: python
      
      >>> User.ALL.insert(
        {'name':'John', 'email':'me@here.org'},
        {'name':'Paul', 'email':'paul@here.org'},
      )
      [42, 43]
    '''
    self = copy. copy(self)
    instances = None
    if args: instances = args[0]
    if len(args)>1:
      raise ValueError('please pass in only one argument (a list of instances or dicts)')
    if instances and data:
      raise ValueError('please pass in only a list or kwargs, not both')
    if instances:
      self._cmd = CMD.INSERT_MANY
      self._insert = instances
    else:
      self._cmd = CMD.INSERT
      self._insert = data
    if self._tbl._dqoi_pk:
      sql, args = self._sql()
      def f(rows):
        if self._cmd == CMD.INSERT:
          rows = list(rows)
          if len(self._tbl._dqoi_pk.columns)==1:
            return rows[0][0] if rows else None
          else:
            return tuple(rows[0]) if rows else None
        else:
          print('rows', rows)
          return list(rows)
      if asyncio.get_running_loop():
        if not self._insert and self._cmd == CMD.INSERT_MANY:
          return self._noop([] if instances else None)
        return self._async_fetch_f(sql, args, f)
      else: 
        if not self._insert and self._cmd == CMD.INSERT_MANY:
          return [] if instances else None
        return self._sync_fetch_f(sql, args, f)
    else:
      return self._execute()

  async def _noop(self, ret):
    return ret
  
  def _sync_fetch_f(self, sql, args, f):
    with self._conn_or_tx_sync as conn:
      return f(conn.sync_fetch(sql, args))
  
  async def _async_fetch_f(self, sql, args, f):
    async with self._conn_or_tx_sync as conn:
      return f(await conn.async_fetch(sql, args))
  
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
    if self._tbl._dqoi_db: return self._tbl._dqoi_db
    return dqo.DB
    
  def _register_tables(self, d):
    d.register(self._tbl)
    for join in self._joins:
      if hasattr(join.other, '_dqoi_db_name'):
        d.register(join.other)
  
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
    if self._cmd==CMD.INSERT_MANY:
      self._insert_many_sql_(d, sql, args)
    if self._cmd==CMD.UPDATE:
      self._update_sql_(d, sql, args)
    if self._cmd==CMD.DELETE:
      self._delete_sql_(d, sql, args)

  def _select_sql_(self, d, sql, args):
    self._register_tables(d)
    sql.write('select ')
    self._gen_select(d, sql, args)
    sql.write(' from ')
    self._tbl._sql_(d, sql, args)
    self._gen_joins(d, sql, args)
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
      
  def _gen_joins(self, d, sql, args):
   for join in self._joins:
    join._sql_(d, sql, args)
  
  def _update_sql_(self, d, sql, args):
    sql.write('update ')
    sql.write(d.term(self._tbl._dqoi_db_name))
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
    sql.write(d.term(self._tbl._dqoi_db_name))
    to_insert = [(k,v) for k,v in self._insert.items() if not k.startswith('_')]
    if to_insert:
      sql.write(' (')
      values = []
      first = True
      for k,v in to_insert:
        column = self._tbl._dqoi_columns_by_attr_name[k]
        if first: first = False
        else: sql.write(',')
        sql.write(d.term(column.name))
        args.append(v)
        values.append(d.arg)
      sql.write(') values (')
      sql.write(','.join(values))
      sql.write(')')
    else:
      sql.write(' default values')
    if self._tbl._dqoi_pk:
      sql.write(' returning ')
      sql.write(','.join([c.name for c in self._tbl._dqoi_pk.columns]))
      
  def _insert_many_sql_(self, d, sql, args):
    if not to_insert:
      # don't fail inserting no rows
      sql.write('select 1 where 1=2')
      return
    raise Exception('TODO')
      
  def _delete_sql_(self, d, sql, args):
    sql.write('delete from ')
    sql.write(d.term(self._tbl._dqoi_db_name))
    self._gen_where(d, sql, args)
            
  def _gen_select(self, d, sql, args):
    components = self._tbl._dqoi_columns if self._select is None else self._select
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


class Join:

  def __init__(self, type, other, on):
    self.type = type
    self.other = other
    self.on = on

  def _sql_(self, d, sql, args):
    sql.write(' ')
    sql.write(self.type)
    sql.write(' join ')
    if isinstance(self.other, InnerQuery):
      sql.write('(')
      self.other._sql_(d, sql, args)
      sql.write(')')
    else:
      self.other._sql_(d, sql, args)
    if self.on:
      sql.write(' on ')
      self.on._sql_(d, sql, args)
      

class AsyncIterable:

  def __init__(self, query):
    self.query = query
    sql, args = query._sql()
    self.keys = [c.name for c in query._select]
    self._inited = False
  
  async def _init(self):
    sql, args = self.query._sql()
    async with self.query._db.connection() as conn:
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
      with query._db.connection() as conn:
        self.iter = list(conn.sync_fetch(sql, args)).__iter__()

  def __next__(self):
    row = self.iter.__next__()
    o = self.query._tbl()
    o.__dict__.update(dict(zip(self.keys, row)))
    return o

import dqo
