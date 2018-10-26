import asyncio

import dqo


def define_tables(db):
  @dqo.Table(db=db)
  class A:
    id = dqo.Column(int, primary_key=True)
  @dqo.Table(db=db)
  class B:
    id = dqo.Column(int, primary_key=True)
    a = dqo.ForeignKey(A.id)
  @dqo.Table(db=db)
  class Something:
    id = dqo.Column(int, primary_key=True)
    col1 = dqo.Column(int)
    col2 = dqo.Column(str)
    col3 = dqo.Column(int, name='col4')
  @dqo.Table(db=db)
  class Something2:
    col1 = dqo.Column(int)
    col2 = dqo.Column(str)
    col3 = dqo.Column(int, name='col4')
  return {k:v for k,v in locals().items() if k!='db'}


class BaseSync:

  @classmethod
  def setUpClass(cls):
    cls.tables = define_tables(cls.db)
    globals().update(cls.tables)
    cls.db.evolve()

  def setUp(self):
    for tbl in self.tables.values():
      tbl.ALL.delete()

  def test_insert_and_select_all(self):
    Something.ALL.insert(col1=1)
    saw_something = False
    for something in Something.ALL:
      self.assertEqual(something.col1, 1)
      saw_something = True
    self.assertTrue(saw_something)

  def test_save_and_select_all(self):
    something = Something()
    something.col1 = 1
    something.save()
    saw_something = False
    for something in Something.ALL:
      self.assertEqual(something.col1, 1)
      saw_something = True
    self.assertTrue(saw_something)

  def test_select_nothing(self):
    self.assertEqual(list(Something.ALL), [])

  def test_first(self):
    self.assertEqual(Something.ALL.first(), None)

  def test_first2(self):
    Something.ALL.insert(col1=1)
    self.assertEqual(Something.ALL.first().col1, 1)

  def test_first_col_diff_name(self):
    Something2.ALL.insert(col3=4)
    o = Something2.ALL.first()
    self.assertEqual(o.col3, 4)
    self.assertEqual(repr(o), '<Something2 col1=None col2=None col3=4>')

  def test_instance_class_name(self):
    Something.ALL.insert(col1=1)
    o = Something.ALL.first()
    self.assertEqual(o.__class__.__name__, 'Something')

  def test_instance_repr(self):
    Something2.ALL.insert(col1=1)
    o = Something2.ALL.first()
    self.assertEqual(repr(o), '<Something2 col1=1 col2=None col3=None>')
    
  def test_order_by(self):
    Something.ALL.insert(col1=1)
    Something.ALL.insert(col1=2)
    o = Something.ALL.order_by(Something.col1).first()
    self.assertEqual(o.col1, 1)
    o = Something.ALL.order_by(Something.col1.desc).first()
    self.assertEqual(o.col1, 2)
    
  def test_count(self):
    Something.ALL.insert(col1=1)
    Something.ALL.insert(col1=2)
    self.assertEqual(Something.ALL.count(), 2)
    
  def test_count_by(self):
    Something.ALL.insert(col1=1)
    Something.ALL.insert(col1=2)
    self.assertEqual(Something.ALL.count_by(Something.col1), {1:1,2:1})
    
  def test_count_by_order(self):
    Something.ALL.insert(col1=1)
    Something.ALL.insert(col1=2)
    Something.ALL.insert(col1=2)
    self.assertEqual(list(Something.ALL.order_by(dqo.sql.count.desc).count_by(Something.col1).items()), [(2,2),(1,1)])
    
  def test_count_by_2_cols(self):
    Something.ALL.insert(col1=1)
    Something.ALL.insert(col1=2)
    self.assertEqual(Something.ALL.count_by(Something.col1, Something.col2), {(1,None):1,(2,None):1})
    
  def test_fk(self):
    a = A()
    a.save()
    self.assertTrue(a.id > 0)
    b = B()
    b.a_id = a.id
    b.save()
    self.assertEqual(B.ALL.first().a_id, a.id)
    
  def test_is_null(self):
    Something.ALL.insert(col1=1)
    o = Something.ALL.where(Something.col2.is_null).first()
    self.assertEqual(o.col1, 1)
    
  def test_is_not_null(self):
    Something.ALL.insert(col1=1)
    o = Something.ALL.where(Something.col1.is_not_null).first()
    self.assertEqual(o.col1, 1)
    
  def test_inner_query(self):
    Something.ALL.insert(col1=1)
    Something.ALL.insert(col3=1)
    o = Something.ALL.where(Something.col3.in_(Something.ALL.select(Something.col1))).first()
    self.assertEqual(o.col3, 1)

