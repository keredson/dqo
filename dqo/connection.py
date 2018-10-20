import threading

TLS = threading.local()

class Connection(object):
  
  def __init__(self, db, get_raw_conn):
    self._get_raw_conn = get_raw_conn
    print('self._get_raw_conn', self._get_raw_conn.__class__.__name__)
    self._raw_conn = None

  async def __aenter__(self):
    print('async entering context')
    self._raw_conn = await self._get_raw_conn()
    return self

  async def __aexit__(self, exc_type, exc, tb):
    print('async exiting context')
    if self._raw_conn:
      await self._raw_conn.close()

  def async_execute(self, sql, args):
    print('async_execute', sql, args)
    return self._raw_conn.execute(sql, *args)
      

  def __enter__(self):
    if self._raw_conn: return OpenConnection(self._raw_conn)
    self._raw_conn = self._get_raw_conn()
    self._raw_conn.autocommit = True
    TLS.conn_or_tx = self
    return self

  def __exit__(self, exc_type, exc, tb):
    if self._raw_conn:
      self._raw_conn.close()
    TLS.conn_or_tx = None
    
  def sync_execute(self, sql, args):
    print('sync_execute', sql, args)
    cur = self._raw_conn.cursor()
    cur.execute(sql, args)
    return
      
  def sync_fetch(self, sql, args):
    print('sync_fetch', sql, args)
    cur = self._raw_conn.cursor()
    cur.execute(sql, args)
    while True:
      rows = cur.fetchmany()
      if not rows: break
      yield from rows
      

class OpenConnection(Connection):
  def __init__(self, _raw_conn):
    self._raw_conn = _raw_conn
  async def __aenter__(self):
    pass
  async def __aexit__(self, exc_type, exc, tb):
    pass
  def __enter__(self):
    return self
  def __exit__(self, exc_type, exc, tb):
    pass
    
