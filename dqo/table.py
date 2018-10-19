import inspect

from .query import Query
from .column import Column
  

class BaseTable(object):
  pass
  
def table(cls):

  class Table(BaseTable):
    _name = cls._name if hasattr(cls, '_name') else cls.__name__.lower()
    _columns = get_columns(cls)

  Table.ALL = Query(Table)
  Table.__name__ = cls.__name__
  for col in Table._columns:
    setattr(Table, col._name, col)
  Table._db = cls._db if hasattr(cls, '_db') else None
  
  return Table


def get_columns(cls):
  ret = []
  for name, value in inspect.getmembers(cls):
    if not isinstance(value, Column): continue
    value._set_name(name)
    ret.append(value)
  return ret
