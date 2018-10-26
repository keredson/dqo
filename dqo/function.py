import copy

from .column import PosColumn, NegColumn


class Function:

  def __init__(self, name, args=None):
    self.name = name
    self._name = name
    self.args = args
    
  def __call__(self, *args):
    self.args = args
    return self.__class__(self.name, args)
    
  def _sql_(self, d, sql, args):
    sql.write(self.name)
    if self.args is not None:
      sql.write('(')
      first = True
      for component in self.args:
        if hasattr(component,'_sql_'):
          component._sql_(d, sql, args)
        else:
          args.append(component)
          sql.write(d.arg)
      sql.write(')')
    
  def __pos__(self):
    return PosColumn(self)

  def __neg__(self):
    return NegColumn(self)

  @property
  def asc(self):
    '''
    Example:

    .. code-block:: python
      
      q = A.ALL.select(A.id, dqo.sql.count(1) \
               .left_join(B, on=A.id==B.a_id).as_('couunt')) \
               .group_by(A.id) \
               .order_by(dqo.sql.count(1).asc)
    '''
    return +self

  @property
  def desc(self):
    '''
    Example:

    .. code-block:: python
      
      q = A.ALL.select(A.id, dqo.sql.count(1) \
               .left_join(B, on=A.id==B.a_id).as_('couunt')) \
               .group_by(A.id) \
               .order_by(dqo.sql.count(1).desc)
    '''
    return -self

  def as_(self, s):
    '''
    Returns this function as a different name.  For instance:

    .. code-block:: python
      
      a = A.ALL.select(A.id, dqo.sql.count(1) \
               .left_join(B, on=A.id==B.a_id).as_('couunt')) \
               .group_by(A.id) \
               .first()
    
    Would give you an ```a.couunt``` with an integer value.

    '''
    self = copy.copy(self)
    self._name = s
    return self


class CountFunction(Function):
  def __init__(self, name, args=None):
    super().__init__(name)
    if not args: args = [1]
    args = [sql(a) if a in (1,'*') else a for a in args]
    self.args = args


class FunctionGenerator:

  def __getattr__(self, fn_name):
    if fn_name.lower()=='count': return CountFunction(fn_name)
    return Function(fn_name)

  def __call__(self, literal):
    return Function(str(literal))

    
sql = FunctionGenerator()
'''
  ``dqo.sql`` is a special object that generates functions and other SQL literals for use in your queries.
  Literals are inserted verbatim into the expressed SQL, so **make sure you never use untrusted data!**
  
  *Code here assumes you've:* ``from dqo import sql``
  
  For example, while:
  
  .. code-block:: python
    
    User.name == 'John'
  
  would generate:
  
  .. code-block:: sql
    
    name = ?
    
  and ``'John'`` would be passed in as an argument to your database library, this:

  .. code-block:: python
    
    User.name == sql.JOHN
      
  would generate:
  
  .. code-block:: sql
    
    name = JOHN

  which probably would not make sense to your database.  A more likely example:
    
  .. code-block:: python
    
    User.name == sql.CONCAT(
      User.first_name, ' ', User.last_name
    )
      
  would generate:
  
  .. code-block:: sql
    
    name = CONCAT(first_name, ' ', last_name)
      
  If your literal isn't a valid Python identifier, pass it in as a parameter:

  .. code-block:: python

    # generates COUNT(*)
    sql.COUNT(sql('*'))
    
  The above illistrates the syntax, but is actually unecessary for ``COUNT()``, which has special checks 
  for ``sql.count('*')`` and ``sql.count(1)`` since they're such common calls.
  
  A common operation on a query might be:
  
  .. code-block:: python

    # TODO
    User.ALL.select(
      +sql.COALESCE(User.name, 'Unknown').as(User.name)
    )
    
  To provide a query-specific default for the user's name.
'''


