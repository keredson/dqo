import datetime

import dqo


class BaseEvolve:

  def setUp(self):
    self.before = self.build_db()
    self.after = self.build_db()
    
  def assertEqualAndWorks(self, changes, target):
    self.assertEqual(changes, target)
    with self.after.connection() as conn:
      conn.execute_all(changes)
    self.assertEqual(self.after.diff(), [])

  def test_new_table(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [('create table something (col1 integer)', [])])

  def test_new_table2(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int, null=False)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [('create table something (col1 integer not null)', [])])

  def test_new_table3(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int, primary_key=True)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [('create table something (col1 serial not null, primary key (col1))', [])])

  def test_col_different_name(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int, name='col2')
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [('create table something (col2 integer)', [])])

  def test_col_default(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int, default=1)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [('create table something (col1 integer default %s)', [1])])

  def test_col_default_sql_fn(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(datetime.datetime, default=dqo.sql.NOW())
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [('create table something (col1 timestamp default NOW())', [])])

  def test_col_tz(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(datetime.datetime, tz=True)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [('create table something (col1 timestamp with time zone)', [])])

  def test_array_col(self):
    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column([str])
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [('create table something (col1 text[])', [])])

  def test_two_column_pk(self):
    @dqo.Table(db=self.after)
    class A:
      part1 = dqo.Column(int)
      part2 = dqo.Column(int)
      _pk = dqo.PrimaryKey(part1, part2)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes,  [
      ('create table a (part1 integer not null, part2 integer not null, primary key (part1,part2))', [])
    ])

  def test_double_pk(self):
    with self.assertRaises(Exception) as context:
      @dqo.Table(db=self.after)
      class A:
        x = dqo.Column(int, primary_key=True)
        y = dqo.Column(int, primary_key=True)
      changes = self.after.diff()
    self.assertTrue('there can be only one' in str(context.exception))

  def test_double_pk2(self):
    with self.assertRaises(Exception) as context:
      @dqo.Table(db=self.after)
      class A:
        x = dqo.Column(int)
        y = dqo.Column(int)
        _pk1 = dqo.PrimaryKey(x)
        _pk2 = dqo.PrimaryKey(y)
      changes = self.after.diff()
    self.assertTrue('there can be only one' in str(context.exception))

  def test_fk(self):
    @dqo.Table(db=self.after)
    class A:
      id = dqo.Column(int, primary_key=True)
    @dqo.Table(db=self.after)
    class B:
      id = dqo.Column(int, primary_key=True)
      a = dqo.ForeignKey(A.id)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('create table a (id serial not null, primary key (id))', []),
      ('create table b (id serial not null, a_id integer not null, primary key (id))', []),
      ('alter table b add foreign key (a_id) references a (id)', []),
    ])

  def test_fake_fk(self):
    @dqo.Table(db=self.after)
    class A:
      id = dqo.Column(int, primary_key=True)
    @dqo.Table(db=self.after)
    class B:
      id = dqo.Column(int, primary_key=True)
      a = dqo.ForeignKey(A.id, fake=True)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('create table a (id serial not null, primary key (id))', []),
      ('create table b (id serial not null, a_id integer not null, primary key (id))', []),
    ])

  def test_double_fks(self):
    @dqo.Table(db=self.after)
    class A:
      id = dqo.Column(int, primary_key=True)
    @dqo.Table(db=self.after)
    class B:
      id = dqo.Column(int, primary_key=True)
      x = dqo.ForeignKey(A.id)
      y = dqo.ForeignKey(A.id)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('create table a (id serial not null, primary key (id))', []),
      ('create table b (id serial not null, x_id integer not null, y_id integer not null, primary key (id))', []),
      ('alter table b add foreign key (x_id) references a (id)', []),
      ('alter table b add foreign key (y_id) references a (id)', []),
    ])

  def test_multi_column_fk(self):
    @dqo.Table(db=self.after)
    class A:
      part1 = dqo.Column(int)
      part2 = dqo.Column(int)
      _pk = dqo.PrimaryKey(part1, part2)
    @dqo.Table(db=self.after)
    class B:
      a = dqo.ForeignKey(A.part1, A.part2)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('create table a (part1 integer not null, part2 integer not null, primary key (part1,part2))', []), 
      ('create table b (a_part1 integer not null, a_part2 integer not null)', []), 
      ('alter table b add foreign key (a_part1,a_part2) references a (part1,part2)', [])
    ])

  def test_index(self):
    @dqo.Table(db=self.after)
    class A:
      col1 = dqo.Column(int, index=True)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('create table a (col1 integer)', []),
      ('create index on a (col1)', []),
    ])

  def test_unique_index(self):
    @dqo.Table(db=self.after)
    class A:
      col1 = dqo.Column(int, unique=True)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('create table a (col1 integer)', []),
      ('create unique index on a (col1)', []),
    ])

  def test_multi_column_index(self):
    @dqo.Table(db=self.after)
    class A:
      col1 = dqo.Column(int)
      col2 = dqo.Column(int)
      _idx = dqo.Index(col1, col2)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('create table a (col1 integer, col2 integer)', []),
      ('create index on a (col1,col2)', []),
    ])

  def test_index_method(self):
    @dqo.Table(db=self.after)
    class A:
      col1 = dqo.Column(int)
      _idx = dqo.Index(col1, method='hash')
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('create table a (col1 integer)', []),
      ('create index on a using hash (col1)', []),
    ])

  def test_index_include(self):
    @dqo.Table(db=self.after)
    class A:
      col1 = dqo.Column(int)
      col2 = dqo.Column(int)
      _idx = dqo.Index(col1, include=[col2])
    changes = self.after.diff()
    # postgres 11 only so don't run
    self.assertEqual(changes, [
      ('create table a (col1 integer, col2 integer)', []),
      ('create index on a (col1) include (col2)', []),
    ])

  def test_index_name(self):
    @dqo.Table(db=self.after)
    class A:
      col1 = dqo.Column(int)
      _idx = dqo.Index(col1, name='woot')
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('create table a (col1 integer)', []),
      ('create index woot on a (col1)', []),
    ])

  def test_drop_table(self):
    @dqo.Table(db=self.before)
    class A:
      col1 = dqo.Column(int)
    self.before.evolve()
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('drop table a', []),
    ])

  def test_rename_table(self):
    @dqo.Table(db=self.before)
    class A:
      col1 = dqo.Column(int)
    self.before.evolve()
    @dqo.Table(db=self.after, aka='a')
    class B:
      col1 = dqo.Column(int)
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('alter table a rename to b', []),
    ])


  def test_rename_column(self):
    @dqo.Table(db=self.before)
    class A:
      col1 = dqo.Column(int)
    self.before.evolve()
    @dqo.Table(db=self.after)
    class A:
      col2 = dqo.Column(int, aka='col1')
    changes = self.after.diff()
    self.assertEqualAndWorks(changes, [
      ('alter table a alter column col1 rename to col2', []),
    ])



