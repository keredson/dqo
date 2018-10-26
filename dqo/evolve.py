import datetime, io

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
    return sorted(to_add), sorted(to_delete), to_rename
  
  def fk_def(self, d, fk, args):
    frm = ','.join([d.term(c.name) for c in fk.frm])
    to = ','.join([d.term(c.name) for c in fk.to])
    return 'foreign key (%s) references %s (%s)' % (frm, fk.to[0].tbl._dqoi_db_name, to)
    
  def create_table(self, table):
    d = self.dialect.for_query()
    args = []
    columns = [self.column_def(d, col, args) for col in table._dqoi_columns]
    if table._dqoi_pk:
      pk_cols = [d.term(c.name) for c in table._dqoi_pk.columns]
      columns.append('primary key (%s)' % ','.join(pk_cols))
    return [('create table %s (%s)' % (d.term(table._dqoi_db_name), ', '.join(columns)), args)]
  
  def add_fks(self, table):
    d = self.dialect.for_query()
    args = []
    fk_defs = [self.fk_def(d, fk, args) for fk in table._dqoi_fks if not fk.fake]
    return [('alter table %s add %s' % (d.term(table._dqoi_db_name), fk_def),[]) for fk_def in fk_defs]
  
  def add_indexes(self, table):
    ret = []
    for index in table._dqoi_indexes:
      d = self.dialect.for_query()
      args = []
      cmd = ['create', 'unique' if index.unique else None, 'index', d.term(index.name) if index.name else None, 'on', d.term(table._dqoi_db_name)]
      if index.method:
        cmd += ['using', ''.join(filter(str.isalnum,index.method))]
      cmd.append('(%s)' % ','.join([d.term(c.name) for c in index.columns]))
      if index.include:
        cmd.append('include (%s)' % ','.join([d.term(c.name) for c in index.include]))
      ret.append((' '.join([s for s in cmd if s]),[]))
    return ret
  
  def column_def(self, d, col, args):
    parts = [d.term(col.name), self.python_to_db_type(col)]
    if not col.null: parts.append('not null')
    if col.default:
      if hasattr(col.default,'_sql_'):
        sql = io.StringIO()
        col.default._sql_(d, sql, args)
        parts.append('default '+ sql.getvalue())
      else:
        parts.append('default '+ d.arg)
        args.append(col.default)
    return ' '.join(parts)
    
  def drop_table(self, name):
    d = self.dialect.for_query()
    return [('drop table %s' % d.term(name),[])]
    
  def rename_table(self, old_name, new_name):
    d = self.dialect.for_query()
    return [('alter table %s rename to %s' % (d.term(old_name), d.term(new_name)),[])]
    
  def diff(self, ignore_tables = []):
  
    to_run = []
    
    defined_tables = self.db._known_tables #+ orphan_tables
    defined_tables_by_name = {tbl._dqoi_db_name:tbl for tbl in defined_tables}
  
    table_adds, table_deletes, table_renames = self.calc_table_changes(defined_tables, ignore_tables=ignore_tables)

    for name in table_adds:
      to_run += self.create_table(defined_tables_by_name[name])
      to_run += self.add_indexes(defined_tables_by_name[name])
    for name in table_adds:
      to_run += self.add_fks(defined_tables_by_name[name])
    for old_name, new_name in table_renames.items():
      to_run += self.rename_table(old_name, new_name)
    for name in table_deletes:
      to_run += self.drop_table(name)
    
    return to_run
  
class DiffPostgres(DiffBase):
  
  python_to_db_type_map = {
    str: 'text',
    int: 'integer',
    float: 'real',
    datetime.date: 'date',
    datetime.datetime: 'timestamp',
  }

  def python_to_db_type(self, col):
    if col.kind==int and col.primary_key==True:
      return 'serial'
    is_array_type = False
    kind = col.kind
    if isinstance(kind, list):
      is_array_type = True
      kind = kind[0]
    db_type = self.python_to_db_type_map[kind]
    if hasattr(col,'tz') and col.tz:
      db_type += ' with time zone'
    if is_array_type:
      db_type += '[]'
    return db_type
    


