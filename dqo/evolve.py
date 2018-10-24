import datetime

from .database import Dialect

def Diff(db):
  if db.dialect==Dialect.POSTGRES:
    return DiffPostgres(db)
  else:
    raise Exception("i don't know how to evolve %s" % db.dialect)
    


class DiffBase:

  def __init__(self, db):
    self.dialect = db.dialect
    self.db = db
    
  def get_existing_table_names(self):
    with self.db.connection() as conn:
      rows = conn.sync_fetch('select tablename from pg_catalog.pg_tables where schemaname=%s order by tablename', ['public'])
      return set([r[0] for r in rows])
  
  def calc_table_changes(self, defined_tables, ignore_tables=None):
    ignore_tables = set() if ignore_tables is None else set(ignore_tables)
    to_rename = {}
    existing_table_names = self.get_existing_table_names()
    akas_by_name = {tbl._dqoi_db_name:tbl._dqoi_aka for tbl in defined_tables}
    defined_table_names = set([tbl._dqoi_db_name for tbl in defined_tables])
    to_add = defined_table_names - existing_table_names - ignore_tables
    to_delete = existing_table_names - defined_table_names - ignore_tables
    for name in list(to_add):
      possible_renames = akas_by_name[name] & to_delete
      if len(possible_renames)==1:
        old_name = possible_renames.pop()
        to_add.remove(name)
        to_delete.remove(old_name)
        to_rename[old_name] = name
      if len(possible_renames)>1:
        raise Exception('table %s has too many possible matches to rename: %s' % (name, possible_renames))
    return to_add, to_delete, to_rename
    
  def create_table(self, table):
    d = self.dialect.for_query()
    args = []
    columns = [self.column_def(d, col, args) for col in table._dqoi_columns]
    return [('create table %s (%s)' % (d.term(table._dqoi_db_name), ', '.join(columns)), args)]
  
  def column_def(self, d, col, args):
    parts = [d.term(col.name), self.python_to_db_type(col)]
    if not col.null: parts.append('not null')
    return ' '.join(parts)
    
  def diff(self, ignore_tables = []):
  
    to_run = []
    
    defined_tables = self.db._known_tables #+ orphan_tables
    defined_tables_by_name = {tbl._dqoi_db_name:tbl for tbl in defined_tables}
  
    table_adds, table_deletes, table_renames = self.calc_table_changes(defined_tables, ignore_tables=ignore_tables)
    print('table_adds, table_deletes, table_renames', table_adds, table_deletes, table_renames)

    for name in table_adds:
      print('add', name)
      to_run += self.create_table(defined_tables_by_name[name])
    
    return to_run
  
class DiffPostgres(DiffBase):
  
  python_to_db_type_map = {
    str: 'text',
    int: 'integer',
    float: 'real',
    datetime.date: 'date',
    datetime.datetime: 'datetime',
  }

  def python_to_db_type(self, col):
    if col.kind==int and col.primary_key==True:
      return 'serial primary key'
    return self.python_to_db_type_map[col.kind]
    


