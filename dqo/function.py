import copy

from .column import PosColumn, NegColumn


class Function:

  def __init__(self, name, args=None):
    self.name = name
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
    return +self

  @property
  def desc(self):
    return -self


class CountFunction(Function):
  def __init__(self, name, args=None):
    self.name = name
    if not args: args = [1]
    args = [sql(a) if a in (1,'*') else a for a in args]
    self.args = args


class FunctionGenerator:

  def __getattr__(self, fn_name):
    if fn_name.lower()=='count': return CountFunction(fn_name)
    return Function(fn_name)

  def __call__(self, arg):
    return Function(str(arg))

    
sql = FunctionGenerator()


