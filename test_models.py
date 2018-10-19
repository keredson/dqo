import dqo

DB = dqo.Database(dialect=dqo.Dialect.POSTGRES('11'))

@dqo.table
class Something:
  col1 = dqo.Column(int)
  col2 = dqo.Column(str)
  _db = DB

