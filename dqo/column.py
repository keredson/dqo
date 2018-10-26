import datetime



class BaseColumn:
  '''
  All columns obey normal python operators.
  
  ======================================== ===============
  Syntax                                   SQL
  ======================================== ===============
  ``User.name == 'John'``                  ``name = 'john'``
  ``User.age < 5``                         ``age < 5``
  ``User.age <= 5``                        ``age <= 5``
  ``User.age > 5``                         ``age > 5``
  ``User.age >= 5``                        ``age >= 5``
  ``User.name != 'John'``                  ``name <> 'john'``
  ``(User.name=='John') & (User.age < 5)`` ``name='john' and age=5``
  ``(User.age < 2) | (User.age >= 65)``    ``age<2 or age>=65``
  ======================================== ===============
  
  In addition, the following condition generators are supported:
  '''

  def __eq__(self, other):
    return Condition('=', [self, other], sep='')

  def __ne__(self, other):
    return Condition('<>', [self, other], sep='')

  def __gt__(self, other):
    return Condition('>', [self, other], sep='')

  def __lt__(self, other):
    return Condition('<', [self, other], sep='')

  def __ge__(self, other):
    return Condition('>=', [self, other], sep='')

  def __le__(self, other):
    return Condition('<=', [self, other], sep='')

  def __and__(self, other):
    return Condition('and', [self, other])

  def __or__(self, other):
    return Condition('or', [self, other])

  @property
  def is_null(self):
    '''
    Returns condition where this column is null.
    '''
    return Condition('is', [self, sql.null])

  @property
  def is_not_null(self):
    '''
    Returns condition where this column is not null.
    '''
    return Condition('is not', [self, sql.null])

  def in_(self, something):
    '''
      Returns a condition where this column is in a list or inner query.
    '''
    return Condition('in', [self, prepare_something(something)])

  def not_in(self):
    '''
      Returns a condition where this column is in a list or inner query.
    '''
    return Condition('not in', [self, prepare_something(something)])

def prepare_something(something):
  if hasattr(something, '_sql_'):
    return InnerQuery(something)


class InnerQuery:
  def __init__(self, query):
    self.query = query
  def _sql_(self, d, sql, args):
    d = d.for_inner_query()
    sql.write('(')
    self.query._sql_(d, sql, args)
    sql.write(')')


def allow_tz(kind):
  return kind in (datetime.datetime,)

class ForeignKey:
  '''
  :param to_columns: The columns this foreign key refers to.
  :param fake: A "fake" foreign key will give you the syntax sugar without actually creating the restriction in the database.  This is sometimes important for performance reasons.
  :param null: If nulls are allowed.

  Defines a foreign key relationship from this table to another.  For example:

  .. code-block:: python

    @dqo.Table()
    class A:
      id = dqo.Column(int, primary_key=True)

    @dqo.Table()
    class B:
      id = dqo.Column(int, primary_key=True)
      a = dqo.ForeignKey(A.id)
  
  IF you want to create a multi-column foreign key, pass in multiple columns:

  .. code-block:: python

    @dqo.Table()
    class B:
      id = dqo.Column(int, primary_key=True)
      my_a = dqo.ForeignKey(A.part1, A.part2)
  
  A foreign key creates one database column for every referenced column, named by a combination of the foreign key name and the 
  columns it's referencing.  So the above example would have columns ``my_a_part1`` and ``my_a_part2`` on table ``b`` as shown:

  .. code-block:: sql

      alter table b add foreign key (my_a_part1,my_a_part2)
      references a (part1,part2)
    '''

  def __init__(self, *to_columns, fake=False, null=True):
    self.to = to_columns
    self._name = None
    self.fake = fake
    self.null = null

  def _gen_columns(self):
    self.frm = []
    for c in self.to:
      c2 = Column(c.kind, null=self.null)
      c2._name = c2.name = '%s_%s' % (self._name, c.name)
      c2.tbl = self.tbl
      if hasattr(c,'tz'):
        c2.tz = c.tz
      self.frm.append(c2)
      self.tbl._dqoi_columns.append(c2)
      setattr(self.tbl, c2._name, c2)
    return self.frm
  

class PrimaryKey:
  '''
  :param columns: The columns this primary key is composed of.

  To identify a single column as a primary key, add ``primary_key=True`` to its column definiton.  For example:
    
  .. code-block:: python

    @dqo.Table()
    class A:
      id = dqo.Column(int, primary_key=True)
      
  This will make the ``id`` column the primary key for the table.
  
  To create a multi-column primary key, use the ``PrimaryKey`` class, passing in the component columns.  For example:

  .. code-block:: python
  
    @dqo.Table()
    class A:
      part1 = dqo.Column(int)
      part2 = dqo.Column(int)
      _pk = dqo.PrimaryKey(part1, part2)
  
  Calling it ``_pk`` isn't necessary or special, just convention.  The name of the variable isn't used, just make sure it doesn't 
  conflict with any column names.
    
  '''
  def __init__(self, *columns):
    for col in columns:
      col.null = False
    self.columns = columns


class Index:
  '''
  :param columns: The columns this index is composed of.
  :param unique: If this is a unique contraint or not.
  :param method: The indexing method (``btree``, ``hash``, ``gist``, ``spgist``, ``gin``, or ``brin`` on PosrgreSQL).
  :param include: A list of non-indexed columns to store in the index.  Postgres 11 feature - see [https://www.postgresql.org/docs/current/static/sql-createindex.html]
  :param name: The name of the index.
  
  Creates an index on the current table.  For example:

  .. code-block:: python

    @dqo.Table()
    class Something:
      col1 = dqo.Column(int)
      col2 = dqo.Column(int)
      _idx1 = dqo.Index(col1, col2, unique=True)
      
  Would generate:

  .. code-block:: sql
  
      create table "a" (col1 integer, col2 integer)
      create unique index on "a" (col1,col2)
      
  Columns have convenience parameters ``index`` and ``unique`` to simplify making single column indexs.  Example:
  
  .. code-block:: python

    @dqo.Table()
    class Something:
      col1 = dqo.Column(int, index=True)
      col2 = dqo.Column(int, unique=True)
  
  '''
  
  def __init__(self, *columns, unique=False, method=None, include=None, name=None):
    self.columns = columns
    self.unique = unique
    self.method = method
    self.include = include
    self.name = name


class Column(BaseColumn):
  '''
  :param kind: A Python type to be mapped to a database column type.  Or a single-element list (containing a type) representing an array column.
  :param name: The database name of the column.
  :param null: If the column can be null or not.
  :param default: The default for the column.  If a constant, stored in the database, else (like a lambda) will be calculated on object creation.
  :param index: If a single column index (w/ the database's default type, ie. BTREE) should be created for this column.
  :param unique: If a single column **UNIQUE** index should be created for this column.
  :param primary_key: If this column should be the primary key for this table.
  :param aka: A string or list of strings with previous names of this column, used for renaming.
  :param tz: If a datatime column, can specify if it's a timezone aware column.
  
  You can create ``ARRAY`` columns with subtypes by passing in a list of a single type.  Example:

  .. code-block:: python
  
    @dqo.Table
    class Product:
      name = dqo.Column(str)
      keywords = dqo.Column([str])
  '''
  
  def __init__(self, kind, name=None, null=True, default=None, index=False, unique=False, primary_key=False, tz=None, aka=None):
    self.kind = kind
    self.primary_key = primary_key
    self.default = default
    self.index = index
    self.unique = unique
    self.null = null
    self.name = name
    self.aka = aka
    if allow_tz(kind):
      self.tz = tz
    if tz is not None and not allow_tz(kind):
      raise ValueError('tz only allowed for datetime columns')
      
  def _set_name(self, name):
    self._name = name
    if not self.name:
      self.name = name
  
  def __pos__(self):
    return PosColumn(self)

  def __neg__(self):
    return NegColumn(self)

  def _sql_(self, d, sql, args):
    sql.write(d.reference(self))

  @property
  def asc(self):
    '''
    The ascending form of the column, used in ``order_by()``.
    '''
    return +self

  @property
  def desc(self):
    '''
    The descending form of the column, used in ``order_by()``.
    '''
    return -self


class PosColumn:
  def __init__(self, column):
    self.column = column
  def _sql_(self, d, sql, args):
    self.column._sql_(d, sql, args)
    sql.write(' asc')
    
class NegColumn:
  def __init__(self, column):
    self.column = column    
  def _sql_(self, d, sql, args):
    self.column._sql_(d, sql, args)
    sql.write(' desc')


class Condition:

  def __init__(self, join, components, sep=' '):
    self._components = components
    self._join = join
    self._sep = sep

  def _sql_(self, d, sql, args):
    first = True
    for component in self._components:
      if first:
        first = False
      else:
        sql.write(self._sep)
        sql.write(self._join)
        sql.write(self._sep)
      if isinstance(component, Condition):
        parens = self._join in ('and','or') and component._join in ('and','or') and self._join!=component._join
        if parens: sql.write('(')
        component._sql_(d, sql, args)
        if parens: sql.write(')')
      elif hasattr(component,'_sql_'):
        component._sql_(d, sql, args)
      else:
        args.append(component)
        sql.write(d.arg)
      
  def __and__(self, other):
    return Condition('and', [self, other])

  def __or__(self, other):
    return Condition('or', [self, other])


from .function import sql

