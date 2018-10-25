import inspect, os, time, timeit

import psycopg2, psycopg2.pool

import dqo

os.system('createdb dqo_benchmark')
db = dqo.Database(sync_src=lambda: psycopg2.connect("dbname='dqo_benchmark'"))

pool = psycopg2.pool.PersistentConnectionPool(1, 2, database = 'dqo_benchmark')
pooled_db = dqo.Database(sync_src=pool)



@dqo.Table(db=db)
class Something:
  id = dqo.Column(int, primary_key=True)
  col1 = dqo.Column(int)

db.evolve()

def test(f, n=1000):
  desc = inspect.getsource(f)
  seconds = timeit.timeit(f, number=n)
  print()
  print(desc.strip())
  print(' ^^^ %i times per second' % (n / seconds))  

def dqo_gen_sql():
  Something.ALL.where(col1=1)._sql()
test(dqo_gen_sql)

def dqo_simple_query():
  list(Something.ALL.where(col1=1))
test(dqo_simple_query, n=400)

def dqo_pooled():
  list(Something.ALL.bind(pooled_db).where(col1=1))
test(dqo_pooled, n=5000)

pool.closeall()


##########
# peewee #
##########
import peewee as pw

# peewee fails w/o this
os.system('psql dqo_benchmark -c "create extension hstore;"')

pwdb = pw.PostgresqlDatabase('dqo_benchmark')
class PeeweeSomething(pw.Model):
  col1 = pw.IntegerField()
  class Meta:
    database = pwdb
    db_table = 'something'


from playhouse.pool import PooledPostgresqlDatabase
pwdb_pooled = PooledPostgresqlDatabase('dqo_benchmark')
class PeeweeSomethingPooled(pw.Model):
  col1 = pw.IntegerField()
  class Meta:
    database = pwdb_pooled
    db_table = 'something'




def peewee_gen_sql():
  PeeweeSomething.select().where(PeeweeSomething.col1==1).sql()
test(peewee_gen_sql)

def peewee_simple_query():
  # https://stackoverflow.com/a/46681567/2449774
  pwdb.connect()
  list(PeeweeSomething.select().where(PeeweeSomething.col1==1))
  pwdb.close()
test(peewee_simple_query, n=400)

def peewee_pooled():
  pwdb_pooled.connect()
  list(PeeweeSomethingPooled.select().where(PeeweeSomethingPooled.col1==1))
  pwdb_pooled.close()
test(peewee_pooled, n=1000)
pwdb_pooled.close_all()


os.system('dropdb dqo_benchmark')

