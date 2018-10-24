import dqo


class BaseEvolve:

  def setUp(self):
    self.before = self.build_db()
    self.after = self.build_db()
    
  def assertEqualsAndWorks(self, changes, target):
    self.assertEqual(changes, target)
    with self.after.connection() as conn:
      conn.execute_all(changes)
    self.assertEqual(self.after.diff(), [])

  def test_new_table(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int)
    changes = self.after.diff()
    self.assertEqualsAndWorks(changes, [('create table something (col1 integer)', [])])

  def test_new_table2(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int, null=False)
    changes = self.after.diff()
    self.assertEqualsAndWorks(changes, [('create table something (col1 integer not null)', [])])

  def test_new_table3(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int, primary_key=True)
    changes = self.after.diff()
    self.assertEqualsAndWorks(changes, [('create table something (col1 serial primary key)', [])])

  def test_col_different_name(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int, name='col2')
    changes = self.after.diff()
    self.assertEqualsAndWorks(changes, [('create table something (col2 integer)', [])])



