import asyncio

import dqo


@dqo.Table
class Something:
  col1 = dqo.Column(int)
  col2 = dqo.Column(str)


class BaseSync:

  def setUp(self):
    Something.ALL.delete()

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

  def test_instance_class_name(self):
    Something.ALL.insert(col1=1)
    o = Something.ALL.first()
    self.assertEqual(o.__class__.__name__, 'Something')

  def test_instance_repr(self):
    Something.ALL.insert(col1=1)
    o = Something.ALL.first()
    self.assertEqual(repr(o), '<Something col1=1 col2=None>')
    
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
    



