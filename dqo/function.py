import copy

class Function:

  def __init__(self, name, args=None):
    self.name = name
    self.args = args
    
  def __call__(self, *args):
    self.args = args
    return Function(self.name, args)
    
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
    

class FunctionGenerator:
  def __getattr__(self, fn_name):
    return Function(fn_name)
    
fn = FunctionGenerator()
