import unittest
import asyncio

import dqo

@dqo.Table()
class Something:
  col1 = dqo.Column(int, primary_key=True)
  col2 = dqo.Column(str)

class SQL(unittest.TestCase):

  def setUp(self):
    self.echo = dqo.DB = dqo.EchoDatabase()

  def test_select_all(self):
    sql, args = Something.ALL._sql()
    self.assertEqual(sql, 'select col1,col2 from something')
    self.assertEqual(args, [])

  def test_select_also_already_selected_col(self):
    sql, args = Something.ALL.select(+Something.col2)._sql()
    self.assertEqual(sql, 'select col1,col2 from something')
    self.assertEqual(args, [])

  def test_select_col1(self):
    sql, args = Something.ALL.select(Something.col1)._sql()
    self.assertEqual(sql, 'select col1 from something')
    self.assertEqual(args, [])

  def test_select_not_col1(self):
    sql, args = Something.ALL.select(-Something.col1)._sql()
    self.assertEqual(sql, 'select col2 from something')
    self.assertEqual(args, [])

  def test_select_nothing(self):
    sql, args = Something.ALL.select()._sql()
    self.assertEqual(sql, 'select  from something') # not valid sql
    self.assertEqual(args, [])

  def test_select_add_col2(self):
    sql, args = Something.ALL.select().select(+Something.col2)._sql()
    self.assertEqual(sql, 'select col2 from something')
    self.assertEqual(args, [])

  def test_where(self):
    sql, args = Something.ALL.where(Something.col1==1)._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=?')
    self.assertEqual(args, [1])

  def test_where_kwargs(self):
    sql, args = Something.ALL.where(col1=1)._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=?')
    self.assertEqual(args, [1])

  def test_where_args_and_kwargs(self):
    sql, args = Something.ALL.where(Something.col1==1, col2=2)._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=? and col2=?')
    self.assertEqual(args, [1,2])

  def test_and(self):
    sql, args = Something.ALL.where((Something.col1==1) & (Something.col2==2))._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=? and col2=?')
    self.assertEqual(args, [1,2])

  def test_or(self):
    sql, args = Something.ALL.where((Something.col1==1) | (Something.col2==2))._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=? or col2=?')
    self.assertEqual(args, [1,2])

  def test_nested_conditional(self):
    sql, args = Something.ALL.where((Something.col1==1) | (Something.col2==2), col1=3)._sql()
    self.assertEqual(sql, 'select col1,col2 from something where (col1=? or col2=?) and col1=?')
    self.assertEqual(args, [1,2,3])

  def test_nested_conditional2(self):
    sql, args = Something.ALL.where((Something.col1==3) | (Something.col1==1) & (Something.col2==2))._sql()
    self.assertEqual(sql, 'select col1,col2 from something where col1=? or (col1=? and col2=?)')
    self.assertEqual(args, [3,1,2])

  def test_delete(self):
    Something.ALL.delete()
    self.assertEqual(self.echo.history, [('delete from something', [])])

  def test_delete2(self):
    Something.ALL.where(col1=2).delete()
    self.assertEqual(self.echo.history, [('delete from something where col1=?', [2])])

  def test_insert(self):
    Something.ALL.insert(col1=1, col2=2)
    self.assertEqual(self.echo.history, [('insert into something (col1,col2) values (?,?) returning col1', [1, 2])])

  def test_insert2(self):
    Something.ALL.insert(**{'col1':1, 'col2':2})
    self.assertEqual(self.echo.history, [('insert into something (col1,col2) values (?,?) returning col1', [1, 2])])

  def test_insert3(self):
    Something(col1=1, col2=2).insert()
    self.assertEqual(self.echo.history, [('insert into something (col1,col2) values (?,?) returning col1', [1, 2])])

  def test_insert4(self):
    Something(col1=1, col2=2).save()
    self.assertEqual(self.echo.history, [('insert into something (col1,col2) values (?,?) returning col1', [1, 2])])

  def test_update(self):
    Something.ALL.set(col1=3, col2=2).where(col1=1).update()
    self.assertEqual(self.echo.history, [('update something set col1=?, col2=? where col1=?', [3,2,1])])

  def test_bind(self):
    db = dqo.EchoDatabase()
    Something.ALL.bind(db).set(col1=3, col2=2).where(col1=1).update()
    self.assertEqual(db.history, [('update something set col1=?, col2=? where col1=?', [3,2,1])])

  def test_insert_via_save(self):
    s = Something()
    s.col1 = 2
    s.save()
    self.assertEqual(self.echo.history, [('insert into something (col1) values (?) returning col1', [2])])

  def test_update_via_save(self):
    s = Something(col1=1).insert()
    s.col2 = 2
    s.save()
    self.assertEqual(self.echo.history, [
      ('insert into something (col1) values (?) returning col1', [1]),
      ('update something set col2=? where col1=?', [2, 1]),
    ])

  def test_select_limit(self):
    sql = Something.ALL.limit(2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something limit ?', [2]))

  def test_select_where_limit(self):
    sql = Something.ALL.where(col1=1).limit(2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something where col1=? limit ?', [1,2]))

  def test_select_first(self):
    sql = Something.ALL.first()
    self.assertEqual(self.echo.history, [('select col1,col2 from something limit ?', [1])])

  def test_order_by(self):
    sql = Something.ALL.order_by(Something.col1)._sql()
    self.assertEqual(sql, ('select col1,col2 from something order by col1', []))

  def test_order_desc(self):
    sql = Something.ALL.order_by(Something.col1.desc)._sql()
    self.assertEqual(sql, ('select col1,col2 from something order by col1 desc', []))
    sql = Something.ALL.order_by(-Something.col1)._sql()
    self.assertEqual(sql, ('select col1,col2 from something order by col1 desc', []))

  def test_order_asc(self):
    sql = Something.ALL.order_by(Something.col1.asc)._sql()
    self.assertEqual(sql, ('select col1,col2 from something order by col1 asc', []))
    sql = Something.ALL.order_by(+Something.col1)._sql()
    self.assertEqual(sql, ('select col1,col2 from something order by col1 asc', []))

  def test_order_by_multiple(self):
    sql = Something.ALL.order_by(Something.col1, -Something.col2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something order by col1, col2 desc', []))

  def test_order_by_clobber(self):
    # order by should always clobber any previous calls to order by on the same query object
    sql = Something.ALL.order_by(Something.col1).order_by(Something.col2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something order by col2', []))

  def test_count(self):
    Something.ALL.count()
    self.assertEqual(self.echo.history, [('select count(1) from something', [])])

  def test_count_by(self):
    Something.ALL.count_by(Something.col1)
    self.assertEqual(self.echo.history, [('select col1,count(1) from something group by col1', [])])

  def test_order_by_count(self):
    Something.ALL.order_by(dqo.sql.count.desc).count_by(Something.col1)
    self.assertEqual(self.echo.history, [('select col1,count(1) from something group by col1 order by count(1) desc', [])])

  def test_count_by_2_cols(self):
    Something.ALL.count_by(Something.col1, Something.col2)
    self.assertEqual(self.echo.history, [('select col1,col2,count(1) from something group by col1,col2', [])])

  def test_select_where_eq(self):
    sql = Something.ALL.where(Something.col1 == 2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something where col1=?', [2]))

  def test_select_where_ne(self):
    sql = Something.ALL.where(Something.col1 != 2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something where col1<>?', [2]))

  def test_select_where_gt(self):
    sql = Something.ALL.where(Something.col1 > 2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something where col1>?', [2]))

  def test_select_where_lt(self):
    sql = Something.ALL.where(Something.col1 < 2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something where col1<?', [2]))

  def test_select_where_gte(self):
    sql = Something.ALL.where(Something.col1 >= 2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something where col1>=?', [2]))

  def test_select_where_lte(self):
    sql = Something.ALL.where(Something.col1 <= 2)._sql()
    self.assertEqual(sql, ('select col1,col2 from something where col1<=?', [2]))



if __name__ == '__main__':
    unittest.main()


