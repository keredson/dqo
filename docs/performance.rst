
Performance
===========

Some micro-bechmarks from ``benchmark.py`` (ops/second):


========================================================= ======= ======
Action                                                    dqo     Peewee
========================================================= ======= ======
Generate SQL (``select * from something where col1=1``)   33963   10867
Run ^^^ (open and close connection)                       298     244
Run ^^^ (connection pool)                                 4660    3147
========================================================= ======= ======



