import inspect, os, timeit

import psycopg2, psycopg2.pool

import dqo

fake_db = dqo.EchoDatabase()
os.system('createdb dqo_test')
real_db = dqo.Database(sync_src=lambda: psycopg2.connect("dbname='dqo_test'"))

pool = psycopg2.pool.PersistentConnectionPool(1, 2, database = 'dqo_test')
pooled_db = dqo.Database(sync_src=pool)

@dqo.Table(db=real_db)
class Something:
  col1 = dqo.Column(int)

real_db.evolve()

def test(f, n=1000):
  desc = inspect.getsource(f)
  seconds = timeit.timeit(f, number=n)
  print('%s ^^^ %i times per second' % (desc, n / seconds))  

test(lambda: list(Something.ALL.bind(fake_db).where(col1=1)))
test(lambda: list(Something.ALL.bind(real_db).where(col1=1)), n=400)
test(lambda: list(Something.ALL.bind(pooled_db).where(col1=1)), n=5000)

pool.closeall()
os.system('dropdb dqo_test')

