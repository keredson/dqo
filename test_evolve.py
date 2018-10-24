import dqo


class BaseEvolve:

  def setUp(self):
    self.before = self.build_db()
    self.after = self.build_db()

  def test_new_table(self):

    @dqo.Table(db=self.after)
    class Something:
      col1 = dqo.Column(int)
      
    changes = self.after.diff_schema()
    self.assertEqual(changes, [('create table something (col1 integer)', [])])
    with self.after.connection() as conn:
      conn.execute_all(changes)


