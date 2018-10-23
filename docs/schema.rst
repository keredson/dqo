Schema Definition
-----------------

We use schema definition syntax simlar to other modern close-to-the-SQL ORMs.  A class represents a table, an
attribute represents a column, etc.  Let's look at an example:

.. code-block:: python

  @dqo.Table
  class User:
    id = dqo.Column(int, primary_key=True)
    name = dqo.Column(str, index=True)
    email = dqo.Column(str, null=False, unique=True)
    
    # 'user' is a reserved word, ie a bad
    # table name so lets define another
    _table_name = 'users'

Note the optional special param ``_table_name`` that can override the default table name derived from the class.

**Multi-column Primary Keys**

The ``primary_key`` parameter for a column is for convenience.  To define a multi-column primary key,
add an attribute to the class:

.. code-block:: python

  @dqo.Table
  class User:
    email = dqo.Column(str)
    dob = dqo.Column(date)
    _pk = dqo.PrimaryKey(User.email, User.dob)

It's an error to define more than one primary key on a table (through either syntax).

**Multi-column Indexes**

Similarly for indexes:

.. code-block:: python

  @dqo.Table
  class User:
    id = dqo.Column(int, primary_key=True)
    name = dqo.Column(str)
    email = dqo.Column(str)
    _idx1 = dqo.Index(User.name, User.email)

**Default Databases for a Table**

If you don't want to use the system-wide defaults, you can define table specific defaults on the table.  Example:

.. code-block:: python

  @dqo.Table
  class User:
    id = dqo.Column(int, primary_key=True)
    name = dqo.Column(str)
    _sync_db = dqo.Database(src=[...])
    _async_db = dqo.Database(src=[...])

