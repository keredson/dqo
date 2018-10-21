

class Comparable:

  def __eq__(self, other):
    return Condition('=', [self, other], sep='')

  def __and__(self, other):
    return Condition('and', [self, other])

  def __or__(self, other):
    return Condition('or', [self, other])

  def __deepcopy__(self, m):
    return self


class Column(Comparable):
  
  def __init__(self, kind, primary_key=False, default=None):
    self.kind = kind
    self.primary_key = primary_key
    self.default = default
    
  def _set_name(self, name):
    self._name = name
    self._db_name = name
  
  def __pos__(self):
    return PosColumn(self)

  def __neg__(self):
    return NegColumn(self)

  def _sql_(self, d, sql, args):
    sql.write(d.term(self._db_name))

  def __deepcopy__(self, m):
    return self
    
  @property
  def asc(self):
    return +self

  @property
  def desc(self):
    return -self


class PosColumn:
  def __init__(self, column):
    self.column = column
  @property
  def _db_name(self):
    return self.column._db_name
    
class NegColumn:
  def __init__(self, column):
    self.column = column    
  @property
  def _db_name(self):
    return self.column._db_name


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
        sql.write(d.arg(len(args)))
      
  def __and__(self, other):
    return Condition('and', [self, other])

  def __or__(self, other):
    return Condition('or', [self, other])
  
  def __deepcopy__(self, m):
    return self
  
