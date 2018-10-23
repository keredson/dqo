API Documentation
=================


.. py:module:: dqo

*Note that all SQL shown is informative, not literal.  This library never directly inserts user provided values in to the SQL.*

Connections
-----------

.. py:class:: Database(src[, dialect=None])

    :param src: A function returning a database connection.
    :param dialect: The database :py:class:`Dilect` to speak (optional).

    The :py:class:`Database` controls connections to your database.  The `src` parameter is required
    
    .. code-block:: python
        
        db = dqo.Database(
          src=lambda: psycopg2.connect("dbname='mydb'"),
        )
        
    If you don't pass in the `dialect` it will be auto-detected by opening and closing a single connection.
    
    You typically assign a database one of three places...
    
    To either (or both) of the global default databases:

    .. code-block:: python

        dqo.DEFAULT_SYNC_DB = ...
        dqo.DEFAULT_ASYNC_DB = ...
    
    As the default for a table:

    .. code-block:: python
      
      @dqo.Table
      class User:
        _sync_db = ...
        _async_db = ...
        
    Or bound to a given query:
    
    .. code-block:: python
    
      User.ALL.bind(db).first()


.. py:class:: Dialect([version=None, lib=None])

    :param version: The database version.  Example: `10`, `'9.2.6'`, etc.
    :param lib: The library used.  Example: `psycopg2`, `asyncpg`, etc.

    All parameters are optional, even calling it as function is optional.  Examples:

    .. code-block:: python
    
        dqo.Dialect.POSTGRES
        dqo.Dialect.POSTGRES(10)
        dqo.Dialect.POSTGRES(version='9.2.6')
        dqo.Dialect.POSTGRES(lib='psycopg2')
        dqo.Dialect.POSTGRES(10, lib=asyncpg)
        

Querying
--------

.. py:class:: Query()

  You don't instantiate this directly, rather every table will have a reference like ``User.ALL``.
  
  All query objects are immutable.  Methods are either builder methods (that return a new query) or terminal (that execute
  SQL on the database).
  
  **Builder Methods**
  
  The following methods all return a new immutable query.

  .. py:method:: bind(db_or_tx)
  
    :param db_or_tx: Either a database or a transaction object.
    
    Specifies what database or connection transaction to use for this query.  For example:
    
    .. code-block:: python
    
      db = dqo.Database(src=...)
      q = User.ALL.where(...).bind(db)
      for user in q:
        # do something
    
  .. py:method:: limit(int)
  
    :param int: The max number of rows to return.
    
    Example:
    
    .. code-block:: python
    
      User.ALL.limit(10)
    
    Equivalent to ``.top(10)``. 
    
  .. py:method:: order_by(*columns)

    Specifies the ordering for the query.  Example:
    
    .. code-block:: python
      
      User.ALL.order_by(User.last_name, User.first_name)
      
    To control the order:
    
    .. code-block:: python
      
      User.ALL.order_by(User.name.asc)
      User.ALL.order_by(User.name.desc)
      
    Every call clobbers any previous calls to ``order_by``.
    
    Only effects ``SELECT`` queries.  Other query types accept but ignore order by directives.  They intentionally don't 
    throw an error, as it's common to change a select into something else as part of a workflow.  Example:
    
    .. code-block:: python
      
      to_delete = User.ALL.where(name='John').order_by(User.kind)
      preview(to_delete) # display who's to be deleted
      to_delete.delete()
    

  .. py:method:: select(*columns)

    :param columns: One or more columns to select.

    By default all columns on a table are selected.  You can customize this by calling ``select()`` with one
    or more columns which should be selected in the query.  For instance, to select *only*:
    
    .. code-block:: python
    
      User.ALL.select(User.id, User.name, User.email)
      
    To select all columns *except* the user's email, prepend the negative operator:
    
    .. code-block:: python
    
      User.ALL.select(-User.email)
      
    To add a column (or something else, like a function) for selection, use the positive operator:
        
    .. code-block:: python
    
      User.ALL.select(+dqo.sql.LOWER(User.first_name))
      
    If you send in an explicit list (no +/-) you will replace the existing selected columns.  If you pass in +/- columns,
    you will modify the existing selected columns.  You cannot do both in the came call.
        
  .. py:method:: set(**kwargs)
    
    Sets values in preperation for an update.  Example:
    
    .. code-block:: python
    
      User.ALL.where(id=1).set(name='John').update()
        
  .. py:method:: top(int)
  
    :param int: The max number of rows to return.
    
    Example:
    
    .. code-block:: python
    
      User.ALL.top(10)
    
    Equivalent to ``.limit(10)``. 
    
  .. py:method:: where(*conditions, **conditions)
  
    Adds conditions to a query.  For example:
    
    .. code-block:: python
    
      User.ALL.where(User.email == 'someone@somewhere.com')
      
    Keyword arguments will be evaluated to fields on the bound table.  For example, the above statement could also be written as:

    .. code-block:: python
    
      User.ALL.where(email='someone@somewhere.com')
    
    Multiple calls to ``where()`` will result in a SQL ``AND``.  For example:
      
    .. code-block:: python
    
      User.ALL.where(name='John').where(email='me@here.com')
      
    Would result in:
      
    .. code-block:: sql
    
      select * from users
      where name='John' and email='me@here.com'
      
    All of the following would also result in the same SQL as above:
    
    .. code-block:: python
    
      User.ALL.where(name='John', email='me@here.com')
      User.ALL.where(User.name=='John', User.email=='me@here.com')
      User.ALL.where(
        (User.name=='John') & (User.email=='me@here.com')
      )
      
    An ``OR`` statment can be created with the bitwise or operator:
    
    .. code-block:: python
    
      User.ALL.where(
        (User.name=='John') | (User.email=='me@here.com')
      )
      
    When using the ``&`` and ``|`` operators, make sure you wrap the condition in parentheses as they have
    lower precedence than others like ``==``.


  **Terminal Methods**
  
  The following methods all execute SQL and return data, or if executed in an ``async`` environment a ``coroutine``.

  .. py:method:: __aiter__()

    All queries are async iterable.  For example:
    
    .. code-block:: python
    
      async for user in User.ALL:
        # do something asynchronously
    
  .. py:method:: __iter__()

    All queries are iterable.  For example:
    
    .. code-block:: python
    
      for user in User.ALL:
        # do something
    
    If in a transaction or a with block defining the scope of the connection, the results will stream.  If a query
    has to open its own connection it will load all records before streaming.  This is because there is no 
    guarantee an iterator will complete, and waiting for the garbage collector is a fool's game.
    
  .. py:method:: count()

    :returns: The number of rows matching the query.
    
    does a ``count(1)`` of the existing query.  Example:
    
    .. code-block:: python
    
      >>> User.ALL.count()
      42

  .. py:method:: count_by(*columns)

    :returns: A ``dict`` where the keys are the db values of the columns selected and the values are their counts.
    
    .. code-block:: python
    
      >>> User.ALL.count_by(User.name)
      {'John':1, 'Paul':2}
      
    If multiple columns are passed, the keys will be tuples.

    .. code-block:: python
    
      >>> User.ALL.count_by(User.first_name, User.last_name)
      {('John','Smith'):1, ('Paul','Anderson'):2}

  .. py:method:: delete()

    :returns: The number of rows deleted.
    
    Deletes the records matched by a given query.  Example:
    
    .. code-block:: python
    
      >>> User.ALL.where(id=1).delete()
      1

  .. py:method:: first()

    :returns: An instance of the selected type or ``None`` if not found.
    
    Adds a ``.limit(1)`` to a given query and returns the first result (if any).  Example:
    
    .. code-block:: python
    
      user = User.ALL.first()

    In async code:
    
    .. code-block:: python
    
      user = await User.ALL.first()

  .. py:method:: insert(*data, **data)
  
    Inserts one or more rows.  If only keyword arguments are passed, a single row is inserted.  For example:

    .. code-block:: python
      
      user = User.ALL.insert(name='John', email='me@here.org')
      
    If a single ``dict`` is passed, a single row is inserted returning the inserted object:

    .. code-block:: python
      
      user = User.ALL.insert({'name':'John', 'email':'me@here.org'})
    
    If multiple ``dicts`` are passed, multiple rows are inserted efficiently in a single query, and a list of users are returned:

    .. code-block:: python
      
      users = User.ALL.insert(
        {'name':'John', 'email':'me@here.org'},
        {'name':'Paul', 'email':'paul@here.org'},
      )

    If a list of ``dicts`` is passed, multiple rows are inserted efficiently in a single query, and a list of users are returned:

    .. code-block:: python
      
      users = User.ALL.insert([
        {'name':'John', 'email':'me@here.org'},
        {'name':'Paul', 'email':'paul@here.org'},
      ])
      
    Instead of ``dicts``, you can also pass in instance objects.

    .. code-block:: python

      users = []
      for i in range(10):
        user = User()
        user.name = 'Me%i' % i
        users.append(user)
        
      users_with_ids = User.ALL.insert(users)
      # users will have their auto-incrementing
      # primary keys set, assuming they have one. 
      
    
    
    
  .. py:method:: update(**kwargs)

    :returns: The number of rows updated.
    
    Executes an update for previously set values.
    
    .. code-block:: python
    
      User.ALL.where(id=1).set(name='John').update()
        


