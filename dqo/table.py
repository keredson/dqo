import inspect

from .query import Query
from .column import Column
  

class BaseTable(object):
  pass
  
class BaseRow(object):

  def __init__(self):
    self.__dict__['_new'] = True
    self.__dict__['_dirty'] = set()

  def __setattr__(self, attr, value):
    dirty = not attr.startswith('_') and (attr not in self.__dict__ or self.__dict__[attr]!=value)
    self.__dict__[attr] = value
    if dirty:
      self.__dict__['_dirty'].add(attr)
    return value

  def save(self):
    if self._new: self.insert()
    else: self.update()
  
  def insert(self):
    self._tbl.ALL.insert(**self.__dict__)
    self.__dict__['_new'] = False
    self.__dict__['_dirty'] = set()
  
  def update(self):
    if not self._tbl._pk: raise Exception("cannot update a row without a primary key")
    self._tbl.ALL.set(**{x:self.__dict__.get(x) for x in self._dirty}).where(*[c==self.__dict__.get(c._name) for c in self._tbl._pk]).update()
    self.__dict__['_dirty'] = set()
  
  def __repr__(self):
    return '<%s %s>' % (self.__class__.__name__, ' '.join(['%s=%s' % (k,repr(v)) for k,v in self.__dict__.items() if not k.startswith('_')]))
  
  
def Table(cls):

  class Table(BaseTable):
    _name = cls._name if hasattr(cls, '_name') else cls.__name__.lower()
    _columns = get_columns(cls)

    def __new__(cls):
      return Row()

    def __instancecheck__(self, instance):
      return isinstance(instance, Row)

  class Row(BaseRow):
    _tbl = Table
    
  Table.ALL = Query(Table)
  Table.__name__ = cls.__name__
  Row.__name__ = cls.__name__
  for col in Table._columns:
    setattr(Table, col._name, col)
  Table._pk = tuple([c for c in Table._columns if c.primary_key])
  Table._db = cls._db if hasattr(cls, '_db') else None
  
  return Table


def get_columns(cls):
  ret = []
  for name, value in inspect.getmembers(cls):
    if not isinstance(value, Column): continue
    value._set_name(name)
    ret.append(value)
  return ret
