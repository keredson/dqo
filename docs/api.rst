API Documentation
=================


.. automodule:: dqo

*Note that most of the SQL shown is informative, not literal.  This library never directly inserts user provided values in to the SQL.*

Connections
-----------

.. autoclass:: Database
  :members:

.. autoclass:: Dialect
  :members:
        

Querying
--------

.. automodule:: dqo.query

.. autoclass:: Query

  You don't instantiate this directly, rather every table will have a reference like ``User.ALL``.
  
  All query objects are immutable.  Methods are either builder methods (that return a new query) or terminal (that execute
  SQL on the database).
  
  **Builder Methods**
  
  The following methods all return a new immutable query.

  .. automethod:: bind
  
  .. automethod:: limit

  .. automethod:: order_by

  .. automethod:: select

  .. automethod:: set

  .. automethod:: top

  .. automethod:: where

  **Terminal Methods**

  The following methods all execute SQL and return data, or if executed in an ``async`` environment a ``coroutine``.

  .. automethod:: __aiter__

  .. automethod:: __iter__

  .. automethod:: count

  .. automethod:: count_by

  .. automethod:: delete

  .. automethod:: first

  .. automethod:: insert

  .. automethod:: update


.. automodule:: dqo
        

Columns and Conditions
----------------------

.. py:class:: Comparable()

  All columns are comparables and obey the normal operators.
  
  ======================================== ===============
  Syntax                                   SQL
  ======================================== ===============
  ``User.name == 'John'``                  ``name = 'john'``
  ``User.age < 5``                         ``age < 5``
  ``User.age <= 5``                        ``age <= 5``
  ``User.age > 5``                         ``age > 5``
  ``User.age >= 5``                        ``age >= 5``
  ``User.name != 'John'``                  ``name <> 'john'``
  ``(User.name=='John') & (User.age < 5)`` ``name='john' and age=5``
  ``(User.age < 2) | (User.age >= 65)``    ``age<2 or age>=65``
  ======================================== ===============
  
  TODO: between, others.

.. py:class:: Column()
  
  .. py:method:: as([alias])

    # TODO

  .. py:method:: in_(list_or_subquery)

    # TODO

  .. py:attribute:: asc
  
    The ascending form of the column, used in ``order_by()``.

  .. py:attribute:: desc
  
    The descending form of the column, used in ``order_by()``.


SQL Functions and Literals
--------------------------

.. autofunction:: sql

  
  
Schema Definition
-----------------

To define a table, add the ``@dqo.Table`` decorator to a class.

.. autofunction:: Table


.. autoclass:: Column
  :members:

.. autoclass:: PrimaryKey
  :members:

.. autoclass:: ForeignKey
  :members:

.. autoclass:: Index
  :members:



