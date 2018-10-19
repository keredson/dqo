from .query import Query
  

class BaseTable(object):

  pass  
  
def table(cls):
  class Table(BaseTable):
    ALL = Query(cls)
  return Table

