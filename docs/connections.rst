
Connections & Pooling
=====================

Pooling - psycopg2
------------------

Example:

.. code-block:: python

  from psycopg2.pool import PersistentConnectionPool
  
  pool = PersistentConnectionPool(1, 20, database='db_name')
  dqo.DB = dqo.Database(sync_src=pool)

Note that if your doing anything other than exiting after you're done (ie. cleaning up unit tests)
you'll want to call ``pool.closeall()`` after your code has run to clean up the pool's connections.
  
  

Pooling - asyncpg
-----------------

.. code-block:: python

  import asyncpg
  
  pool = asyncpg.create_pool(database='db_name')
  dqo.DB = dqo.Database(async_src=pool)

Note that if your doing anything other than exiting after you're done (ie. cleaning up unit tests)
you'll want to call ``await pool.close()`` after your code has run to clean up the pool's connections.

  

