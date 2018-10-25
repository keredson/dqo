import datetime

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

  def test_col_default(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int, default=1)
    changes = self.after.diff()
    self.assertEqualsAndWorks(changes, [('create table something (col1 integer default %s)', [1])])

  def test_col_default_sql_fn(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(datetime.datetime, default=dqo.sql.NOW())
    changes = self.after.diff()
    self.assertEqualsAndWorks(changes, [('create table something (col1 timestamp default NOW())', [])])

  def test_col_tz(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(datetime.datetime, tz=True)
    changes = self.after.diff()
    self.assertEqualsAndWorks(changes, [('create table something (col1 timestamp with time zone)', [])])

  def test_array_col(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column([str])
    changes = self.after.diff()
    self.assertEqualsAndWorks(changes, [('create table something (col1 text[])', [])])

  def test_fk(self):
    @dqo.Table(db=self.after)
    class A:
      id = dqo.Column(int, primary_key=True)
    @dqo.Table(db=self.after)
    class B:
      id = dqo.Column(int, primary_key=True)
      a = dqo.ForeignKey(A.id)
    changes = self.after.diff()
    self.assertEqualsAndWorks(changes, [
      ('create table "a" ("id" serial primary key)', []),
      ('create table b ("id" serial primary key, a_id integer)', []),
      ('alter table b add foreign key (a_id) references a ("id")', []),
    ])


