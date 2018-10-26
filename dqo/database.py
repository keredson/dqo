import asyncio, copy, enum, inspect, io, types

from .connection import Connection
    
class Database(object):
  '''
    :param src: A function returning a database connection, or a connection pool.
    :param dialect: The database :py:class:`Dilect` to speak (optional).

    The :py:class:`Database` controls connections to your database.  The `src` parameter is required.  For example:
    
    .. code-block:: python
        
        db = dqo.Database(
          src=lambda: psycopg2.connect("dbname='mydb'"),
        )
        
    If you're connecting with the async library ``asyncpg``:
        
    .. code-block:: python
        
        db = dqo.Database(
          src=lambda: asyncpg.connect(database='mydb')
        )
        
    If you're doing ``asyncpg`` connection pooling:
    
    .. code-block:: python
        
        db = dqo.Database(
          src=asyncpg.create_pool(database='mydb')
        )
     
    If you don't pass in the `dialect` it will be auto-detected by opening and closing a single connection.
    
    You typically assign a database one of three places...
    
    To either (or both) of the global default databases:

    .. code-block:: python

        dqo.SYNC_DB = ...
        dqo.ASYNC_DB = ...
    
    As the default for a table:

    .. code-block:: python
      
      @dqo.Table(sync_db=[...], async_db=[...])
      class User:
        [...]
        
    Or bound to a given query:
    
    .. code-block:: python
    
      User.ALL.bind(sync_db=db).first()
  '''
  
  def __init__(self, sync_src=None, async_src=None, sync_dialect=None, async_dialect=None):
    self.sync_src = sync_src
    self.async_src = async_src
    self.sync_dialect = sync_dialect
    self.async_dialect = async_dialect

    self._known_tables = []
    self._async_init = None
    self._async_init_lock = asyncio.Lock()
    
    if async_src.__class__.__module__.startswith('asyncpg') and async_src.__class__.__name__=='Pool':
      async def f():
        async with self._async_init_lock:
          if self._async_init:
            pool = await self.async_src
            self.async_src = lambda: pool.acquire()
            self._async_init = None
          return await pool.acquire()
      self._async_init = f

    if sync_src.__class__.__module__.startswith('psycopg2') and 'Pool' in sync_src.__class__.__name__:
      self.sync_src = self._fix_psycopg2_connection_pool(sync_src)
      self.sync_dialect = Dialect.POSTGRES(lib='psycopg2')

    if sync_src and not sync_dialect:
      self._detect_sync_dialect(self.sync_src)
    if async_src and not async_dialect:
      self._detect_async_dialect(self.async_src)
      
  @property
  def dialect(self):
    '''
      This property returns the dialect associated with this database. (It auto-switches between sync and async depending on context.)
    '''
    if asyncio.get_running_loop(): return self.async_dialect
    else: return self.sync_dialect

  def _fix_psycopg2_connection_pool(self, pool):
    class Connection:
      def __init__(self):
        self.conn = pool.getconn()
      def cursor(self): return self.conn.cursor()
      def close(self):
        pool.putconn(self.conn)
    return lambda: Connection()
  
  
  def _detect_sync_dialect(self, src):

    conn_or_pool = src()
    
    if conn_or_pool.__class__.__module__.startswith('psycopg2'):
      self.sync_dialect = Dialect.POSTGRES(lib='psycopg2')
    
    # did we open a connection?
    if hasattr(conn_or_pool, 'close'):
      conn_or_pool.close()
      
    if not self.sync_dialect:
      raise Exception('could not detect the sync database dialect - please open an issue at https://github.com/keredson/dqo')
    
  def _detect_async_dialect(self, src):

    if src.__class__.__module__.startswith('asyncpg') and src.__class__.__name__=='Pool':
      # this is an asyncpg connection pool
      self.async_dialect = Dialect.POSTGRES(lib='asyncpg')
      async def f():
        async with self._async_init_lock:
          if self._async_init:
            #await self.src
            pool = await self.async_src
            self.src = lambda: pool.acquire()
            self._async_init = None
          return await pool.acquire()
      self._async_init = f
      return
    
    conn_or_pool = src()
    
    if inspect.iscoroutine(conn_or_pool):
      event_loop = asyncio.new_event_loop()
      async def f():
        conn_or_pool2 = await conn_or_pool
        if conn_or_pool2.__class__.__module__.startswith('asyncpg'):
          self.async_dialect = Dialect.POSTGRES(lib='asyncpg')
        await conn_or_pool2.close()
      coro = asyncio.coroutine(f)
      event_loop.run_until_complete(f())
      event_loop.close()

    if conn_or_pool.__class__.__module__.startswith('asyncpg'):
      self.async_dialect = Dialect.POSTGRES(lib='asyncpg')

    if not self.dialect:
      raise Exception('could not detect the async database dialect - please open an issue at https://github.com/keredson/dqo')
    
  def connection(self):
    if self._async_init: return Connection(self, self._async_init)
    if asyncio.get_running_loop():
      return Connection(self, self.async_src)
    else: 
      return Connection(self, self.sync_src)
    
  def transaction(self):
    pass
  
  def evolve(self):
    changes = self.diff()
    with self.connection() as conn:
      conn.execute_all(changes)
    
  def diff(self):
    diff = Diff(self)
    changes = diff.diff()
    return changes
  
  
  

class EchoDatabase(Database):
  
  def __init__(self):
    self.sync_dialect = Dialect.GENERIC
    self.sync_src = self.conn
    self.history = []
    self._async_init = None
  
  class Connection:
    def __init__(self, db):
      self.db = db
    def cursor(self): return self
    def close(self): pass
    def execute(self, sql, args):
      self.db.history.append((sql, args))
    def fetchmany(self):
      return []
  
  def conn(self):  
    return EchoDatabase.Connection(self)
    
  

class GenericDialect(object):

  KEYWORDS = set('''
    ADD ADD CONSTRAINT ALTER ALTER COLUMN ALTER TABLE ALL AND ANY AS ASC BETWEEN CASE CHECK COLUMN CONSTRAINT CREATE CREATE DATABASE CREATE INDEX CREATE OR REPLACE VIEW CREATE TABLE CREATE 
    PROCEDURE CREATE UNIQUE INDEX CREATE VIEW DATABASE DEFAULT DELETE DESC DISTINCT DROP DROP COLUMN DROP CONSTRAINT DROP DATABASE DROP DEFAULT DROP INDEX DROP TABLE DROP VIEW EXEC EXISTS 
    FOREIGN KEY FROM FULL OUTER JOIN GROUP BY HAVING IN INDEX INNER JOIN INSERT INTO INSERT INTO SELECT IS NULL IS NOT NULL JOIN LEFT JOIN LIKE LIMIT NOT NOT NULL OR ORDER BY OUTER JOIN 
    PRIMARY KEY PROCEDURE RIGHT JOIN ROWNUM SELECT SELECT DISTINCT SELECT INTO SELECT TOP SET TABLE TOP TRUNCATE TABLE UNION UNION ALL UNIQUE UPDATE VALUES VIEW WHERE
  '''.lower().split())

  def __init__(self):
    self.version = None
    self.lib = None
  
  def __call__(self, version=None, lib=None):
    self = copy.copy(self)
    self.version = version or self.version
    if isinstance(lib, types.ModuleType):
      lib = lib.__name__
    self.lib = lib or self.lib
    return self
    
  def __eq__(self, other):
    same_class = self.__class__ == other.__class__
    return same_class
  
  def term(self, s):
    s = s.lower().replace('"','')
    if s not in self.KEYWORDS: return s
    else: return '"%s"' % s

  @property
  def arg(self):
    self.arg_counter += 1
    if self.lib=='psycopg2': return '%s'
    elif self.lib=='asyncpg': return '$%i' % self.arg_counter
    else: return '?'

  def register(self, tbl):
    if tbl in self.registered: raise ValueError('already registered')
    self.registered[tbl] = self.gen_name(tbl._dqoi_db_name)
  
  def reference(self, col):
    if not self.registered: return self.term(col.name)
    return '%s.%s' % (self.term(self.registered[col.tbl]), self.term(col.name))

  def gen_name(self, name):
    base = ''.join([s[0] for s in name.split('_')])
    i = 1
    while True:
      proposal = '%s%i' % (base,i)
      if proposal not in self.seen:
        self.seen.add(proposal)
        return proposal
      i += 1
      
  def for_query(self):
    self = copy.copy(self)
    self.arg_counter = 0
    self.seen = set()
    self.registered = {}
    return self
  
  def for_inner_query(self):
    return InnerDialect(self)

class InnerDialect:
  def __init__(self, d):
    self.d = d
    self.seen = set()
    self.registered = {}

  def for_inner_query(self):
    return InnerDialect(self)

  def term(self, s):
    return self.d.term(s)

  @property
  def arg(self):
    return self.d.arg

  register = GenericDialect.register
  reference = GenericDialect.reference
  gen_name = GenericDialect.gen_name
  

class PostgresDialect(GenericDialect):
  KEYWORDS = set('''
    ABORT ABS ABSENT ABSOLUTE ACCESS ACCORDING ACTION ADA ADD ADMIN AFTER AGGREGATE ALL ALLOCATE ALSO ALTER ALWAYS ANALYSE ANALYZE AND ANY ARE ARRAY ARRAY_AGG ARRAY_MAX_CARDINALITY AS ASC 
    ASENSITIVE ASSERTION ASSIGNMENT ASYMMETRIC AT ATOMIC ATTRIBUTE ATTRIBUTES AUTHORIZATION AVG BACKWARD BASE64 BEFORE BEGIN BEGIN_FRAME BEGIN_PARTITION BERNOULLI BETWEEN BIGINT BINARY BIT 
    BIT_LENGTH BLOB BLOCKED BOM BOOLEAN BOTH BREADTH BY C CACHE CALL CALLED CARDINALITY CASCADE CASCADED CASE CAST CATALOG CATALOG_NAME CEIL CEILING CHAIN CHAR CHARACTER CHARACTERISTICS 
    CHARACTERS CHARACTER_LENGTH CHARACTER_SET_CATALOG CHARACTER_SET_NAME CHARACTER_SET_SCHEMA CHAR_LENGTH CHECK CHECKPOINT CLASS CLASS_ORIGIN CLOB CLOSE CLUSTER COALESCE COBOL COLLATE COLLATION 
    COLLATION_CATALOG COLLATION_NAME COLLATION_SCHEMA COLLECT COLUMN COLUMNS COLUMN_NAME COMMAND_FUNCTION COMMAND_FUNCTION_CODE COMMENT COMMENTS COMMIT COMMITTED CONCURRENTLY CONDITION 
    CONDITION_NUMBER CONFIGURATION CONFLICT CONNECT CONNECTION CONNECTION_NAME CONSTRAINT CONSTRAINTS CONSTRAINT_CATALOG CONSTRAINT_NAME CONSTRAINT_SCHEMA CONSTRUCTOR CONTAINS CONTENT CONTINUE 
    CONTROL CONVERSION CONVERT COPY CORR CORRESPONDING COST COUNT COVAR_POP COVAR_SAMP CREATE CROSS CSV CUBE CUME_DIST CURRENT CURRENT_CATALOG CURRENT_DATE CURRENT_DEFAULT_TRANSFORM_GROUP 
    CURRENT_PATH CURRENT_ROLE CURRENT_ROW CURRENT_SCHEMA CURRENT_TIME CURRENT_TIMESTAMP CURRENT_TRANSFORM_GROUP_FOR_TYPE CURRENT_USER CURSOR CURSOR_NAME CYCLE DATA DATABASE DATALINK DATE 
    DATETIME_INTERVAL_CODE DATETIME_INTERVAL_PRECISION DAY DB DEALLOCATE DEC DECIMAL DECLARE DEFAULT DEFAULTS DEFERRABLE DEFERRED DEFINED DEFINER DEGREE DELETE DELIMITER DELIMITERS DENSE_RANK 
    DEPENDS DEPTH DEREF DERIVED DESC DESCRIBE DESCRIPTOR DETERMINISTIC DIAGNOSTICS DICTIONARY DISABLE DISCARD DISCONNECT DISPATCH DISTINCT DLNEWCOPY DLPREVIOUSCOPY DLURLCOMPLETE 
    DLURLCOMPLETEONLY DLURLCOMPLETEWRITE DLURLPATH DLURLPATHONLY DLURLPATHWRITE DLURLSCHEME DLURLSERVER DLVALUE DO DOCUMENT DOMAIN DOUBLE DROP DYNAMIC DYNAMIC_FUNCTION DYNAMIC_FUNCTION_CODE 
    EACH ELEMENT ELSE EMPTY ENABLE ENCODING ENCRYPTED END END-EXEC END_FRAME END_PARTITION ENFORCED ENUM EQUALS ESCAPE EVENT EVERY EXCEPT EXCEPTION EXCLUDE EXCLUDING EXCLUSIVE EXEC EXECUTE 
    EXISTS EXP EXPLAIN EXPRESSION EXTENSION EXTERNAL EXTRACT FALSE FAMILY FETCH FILE FILTER FINAL FIRST FIRST_VALUE FLAG FLOAT FLOOR FOLLOWING FOR FORCE FOREIGN FORTRAN FORWARD FOUND FRAME_ROW 
    FREE FREEZE FROM FS FULL FUNCTION FUNCTIONS FUSION G GENERAL GENERATED GET GLOBAL GO GOTO GRANT GRANTED GREATEST GROUP GROUPING GROUPS HANDLER HAVING HEADER HEX HIERARCHY HOLD HOUR 
    IDENTITY IF IGNORE ILIKE IMMEDIATE IMMEDIATELY IMMUTABLE IMPLEMENTATION IMPLICIT IMPORT IN INCLUDING INCREMENT INDENT INDEX INDEXES INDICATOR INHERIT INHERITS INITIALLY INLINE INNER INOUT 
    INPUT INSENSITIVE INSERT INSTANCE INSTANTIABLE INSTEAD INT INTEGER INTEGRITY INTERSECT INTERSECTION INTERVAL INTO INVOKER IS ISNULL ISOLATION JOIN K KEY KEY_MEMBER KEY_TYPE LABEL LAG 
    LANGUAGE LARGE LAST LAST_VALUE LATERAL LEAD LEADING LEAKPROOF LEAST LEFT LENGTH LEVEL LIBRARY LIKE LIKE_REGEX LIMIT LINK LISTEN LN LOAD LOCAL LOCALTIME LOCALTIMESTAMP LOCATION LOCATOR LOCK 
    LOCKED LOGGED LOWER M MAP MAPPING MATCH MATCHED MATERIALIZED MAX MAXVALUE MAX_CARDINALITY MEMBER MERGE MESSAGE_LENGTH MESSAGE_OCTET_LENGTH MESSAGE_TEXT METHOD MIN MINUTE MINVALUE MOD MODE 
    MODIFIES MODULE MONTH MORE MOVE MULTISET MUMPS NAME NAMES NAMESPACE NATIONAL NATURAL NCHAR NCLOB NESTING NEW NEXT NFC NFD NFKC NFKD NIL NO NONE NORMALIZE NORMALIZED NOT NOTHING NOTIFY 
    NOTNULL NOWAIT NTH_VALUE NTILE NULL NULLABLE NULLIF NULLS NUMBER NUMERIC OBJECT OCCURRENCES_REGEX OCTETS OCTET_LENGTH OF OFF OFFSET OIDS OLD ON ONLY OPEN OPERATOR OPTION OPTIONS OR ORDER 
    ORDERING ORDINALITY OTHERS OUT OUTER OUTPUT OVER OVERLAPS OVERLAY OVERRIDING OWNED OWNER P PAD PARALLEL PARAMETER PARAMETER_MODE PARAMETER_NAME PARAMETER_ORDINAL_POSITION 
    PARAMETER_SPECIFIC_CATALOG PARAMETER_SPECIFIC_NAME PARAMETER_SPECIFIC_SCHEMA PARSER PARTIAL PARTITION PASCAL PASSING PASSTHROUGH PASSWORD PATH PERCENT PERCENTILE_CONT PERCENTILE_DISC 
    PERCENT_RANK PERIOD PERMISSION PLACING PLANS PLI POLICY PORTION POSITION POSITION_REGEX POWER PRECEDES PRECEDING PRECISION PREPARE PREPARED PRESERVE PRIMARY PRIOR PRIVILEGES PROCEDURAL 
    PROCEDURE PROGRAM PUBLIC QUOTE RANGE RANK READ READS REAL REASSIGN RECHECK RECOVERY RECURSIVE REF REFERENCES REFERENCING REFRESH REGR_AVGX REGR_AVGY REGR_COUNT REGR_INTERCEPT REGR_R2 
    REGR_SLOPE REGR_SXX REGR_SXY REGR_SYY REINDEX RELATIVE RELEASE RENAME REPEATABLE REPLACE REPLICA REQUIRING RESET RESPECT RESTART RESTORE RESTRICT RESULT RETURN RETURNED_CARDINALITY 
    RETURNED_LENGTH RETURNED_OCTET_LENGTH RETURNED_SQLSTATE RETURNING RETURNS REVOKE RIGHT ROLE ROLLBACK ROLLUP ROUTINE ROUTINE_CATALOG ROUTINE_NAME ROUTINE_SCHEMA ROW ROWS ROW_COUNT 
    ROW_NUMBER RULE SAVEPOINT SCALE SCHEMA SCHEMA_NAME SCOPE SCOPE_CATALOG SCOPE_NAME SCOPE_SCHEMA SCROLL SEARCH SECOND SECTION SECURITY SELECT SELECTIVE SELF SENSITIVE SEQUENCE SEQUENCES 
    SERIALIZABLE SERVER SERVER_NAME SESSION SESSION_USER SET SETOF SETS SHARE SHOW SIMILAR SIMPLE SIZE SKIP SMALLINT SNAPSHOT SOME SOURCE SPACE SPECIFIC SPECIFICTYPE SPECIFIC_NAME SQL SQLCODE 
    SQLERROR SQLEXCEPTION SQLSTATE SQLWARNING SQRT STABLE STANDALONE START STATE STATEMENT STATIC STATISTICS STDDEV_POP STDDEV_SAMP STDIN STDOUT STORAGE STRICT STRIP STRUCTURE STYLE 
    SUBCLASS_ORIGIN SUBMULTISET SUBSTRING SUBSTRING_REGEX SUCCEEDS SUM SYMMETRIC SYSID SYSTEM SYSTEM_TIME SYSTEM_USER T TABLE TABLES TABLESAMPLE TABLESPACE TABLE_NAME TEMP TEMPLATE TEMPORARY 
    TEXT THEN TIES TIME TIMESTAMP TIMEZONE_HOUR TIMEZONE_MINUTE TO TOKEN TOP_LEVEL_COUNT TRAILING TRANSACTION TRANSACTIONS_COMMITTED TRANSACTIONS_ROLLED_BACK TRANSACTION_ACTIVE TRANSFORM 
    TRANSFORMS TRANSLATE TRANSLATE_REGEX TRANSLATION TREAT TRIGGER TRIGGER_CATALOG TRIGGER_NAME TRIGGER_SCHEMA TRIM TRIM_ARRAY TRUE TRUNCATE TRUSTED TYPE TYPES UESCAPE UNBOUNDED UNCOMMITTED 
    UNDER UNENCRYPTED UNION UNIQUE UNKNOWN UNLINK UNLISTEN UNLOGGED UNNAMED UNNEST UNTIL UNTYPED UPDATE UPPER URI USAGE USER USER_DEFINED_TYPE_CATALOG USER_DEFINED_TYPE_CODE 
    USER_DEFINED_TYPE_NAME USER_DEFINED_TYPE_SCHEMA USING VACUUM VALID VALIDATE VALIDATOR VALUE VALUES VALUE_OF VARBINARY VARCHAR VARIADIC VARYING VAR_POP VAR_SAMP VERBOSE VERSION VERSIONING
    VIEW VIEWS VOLATILE WHEN WHENEVER WHERE WHITESPACE WIDTH_BUCKET WINDOW WITH WITHIN WITHOUT WORK WRAPPER WRITE XML XMLAGG XMLATTRIBUTES XMLBINARY XMLCAST XMLCOMMENT XMLCONCAT XMLDECLARATION 
    XMLDOCUMENT XMLELEMENT XMLEXISTS XMLFOREST XMLITERATE XMLNAMESPACES XMLPARSE XMLPI XMLQUERY XMLROOT XMLSCHEMA XMLSERIALIZE XMLTABLE XMLTEXT XMLVALIDATE YEAR YES ZONE
  '''.lower().split())
    
    
    
class Dialect(object):
  '''
    :param version: The database version.  Example: `10`, `'9.2.6'`, etc.
    :param lib: The library used.  Example: `psycopg2`, `asyncpg`, etc.

    All parameters are optional, even calling it as function is optional.  Examples:

    .. code-block:: python
    
        dqo.Dialect.POSTGRES
        dqo.Dialect.POSTGRES(10)
        dqo.Dialect.POSTGRES(version='9.2.6')
        dqo.Dialect.POSTGRES(lib='psycopg2')
        dqo.Dialect.POSTGRES(10, lib=asyncpg)
  '''
  GENERIC = GenericDialect()
  POSTGRES = PostgresDialect()
  

from .evolve import Diff

