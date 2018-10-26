import unittest
import asyncio

import dqo

from test_sync import define_tables

tbls = define_tables(None)
A = tbls['A']
B = tbls['B']

@dqo.Table()
class Something:
  col1 = dqo.Column(int, primary_key=True)
  col2 = dqo.Column(str)

@dqo.Table()
class Something2:
  id = dqo.Column(int, primary_key=True)
  col1 = dqo.Column(int, name='col2')

class SQL(unittest.TestCase):

  def setUp(self):
    self.echo = dqo.DB = dqo.EchoDatabase()

  def test_select_all(self):
    sql, args = Something.ALL._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1')
    self.assertEqual(args, [])

  def test_select_also_already_selected_col(self):
    sql, args = Something.ALL.select(+Something.col2)._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1')
    self.assertEqual(args, [])

  def test_select_col1(self):
    sql, args = Something.ALL.select(Something.col1)._sql()
    self.assertEqual(sql, 'select s1.col1 from something as s1')
    self.assertEqual(args, [])

  def test_select_not_col1(self):
    sql, args = Something.ALL.select(-Something.col1)._sql()
    self.assertEqual(sql, 'select s1.col2 from something as s1')
    self.assertEqual(args, [])

  def test_select_nothing(self):
    sql, args = Something.ALL.select()._sql()
    self.assertEqual(sql, 'select  from something as s1') # not valid sql
    self.assertEqual(args, [])

  def test_select_add_col2(self):
    sql, args = Something.ALL.select().select(+Something.col2)._sql()
    self.assertEqual(sql, 'select s1.col2 from something as s1')
    self.assertEqual(args, [])

  def test_where(self):
    sql, args = Something.ALL.where(Something.col1==1)._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1 where s1.col1=?')
    self.assertEqual(args, [1])

  def test_where_kwargs(self):
    sql, args = Something.ALL.where(col1=1)._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1 where s1.col1=?')
    self.assertEqual(args, [1])

  def test_where_args_and_kwargs(self):
    sql, args = Something.ALL.where(Something.col1==1, col2=2)._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1 where s1.col1=? and s1.col2=?')
    self.assertEqual(args, [1,2])

  def test_and(self):
    sql, args = Something.ALL.where((Something.col1==1) & (Something.col2==2))._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1 where s1.col1=? and s1.col2=?')
    self.assertEqual(args, [1,2])

  def test_or(self):
    sql, args = Something.ALL.where((Something.col1==1) | (Something.col2==2))._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1 where s1.col1=? or s1.col2=?')
    self.assertEqual(args, [1,2])

  def test_nested_conditional(self):
    sql, args = Something.ALL.where((Something.col1==1) | (Something.col2==2), col1=3)._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1 where (s1.col1=? or s1.col2=?) and s1.col1=?')
    self.assertEqual(args, [1,2,3])

  def test_nested_conditional2(self):
    sql, args = Something.ALL.where((Something.col1==3) | (Something.col1==1) & (Something.col2==2))._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1 where s1.col1=? or (s1.col1=? and s1.col2=?)')
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
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 limit ?', [2]))

  def test_select_where_limit(self):
    sql = Something.ALL.where(col1=1).limit(2)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 where s1.col1=? limit ?', [1,2]))

  def test_select_first(self):
    sql = Something.ALL.first()
    self.assertEqual(self.echo.history, [('select s1.col1,s1.col2 from something as s1 limit ?', [1])])

  def test_order_by(self):
    sql = Something.ALL.order_by(Something.col1)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 order by s1.col1', []))

  def test_order_desc(self):
    sql = Something.ALL.order_by(Something.col1.desc)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 order by s1.col1 desc', []))
    sql = Something.ALL.order_by(-Something.col1)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 order by s1.col1 desc', []))

  def test_order_asc(self):
    sql = Something.ALL.order_by(Something.col1.asc)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 order by s1.col1 asc', []))
    sql = Something.ALL.order_by(+Something.col1)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 order by s1.col1 asc', []))

  def test_order_by_multiple(self):
    sql = Something.ALL.order_by(Something.col1, -Something.col2)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 order by s1.col1, s1.col2 desc', []))

  def test_order_by_clobber(self):
    # order by should always clobber any previous calls to order by on the same query object
    sql = Something.ALL.order_by(Something.col1).order_by(Something.col2)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 order by s1.col2', []))

  def test_count(self):
    Something.ALL.count()
    self.assertEqual(self.echo.history, [('select count(1) from something as s1', [])])

  def test_count_by(self):
    Something.ALL.count_by(Something.col1)
    self.assertEqual(self.echo.history, [('select s1.col1,count(1) from something as s1 group by s1.col1', [])])

  def test_order_by_count(self):
    Something.ALL.order_by(dqo.sql.count.desc).count_by(Something.col1)
    self.assertEqual(self.echo.history, [('select s1.col1,count(1) from something as s1 group by s1.col1 order by count(1) desc', [])])

  def test_count_by_2_cols(self):
    Something.ALL.count_by(Something.col1, Something.col2)
    self.assertEqual(self.echo.history, [('select s1.col1,s1.col2,count(1) from something as s1 group by s1.col1,s1.col2', [])])

  def test_select_where_eq(self):
    sql = Something.ALL.where(Something.col1 == 2)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 where s1.col1=?', [2]))

  def test_select_where_ne(self):
    sql = Something.ALL.where(Something.col1 != 2)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 where s1.col1<>?', [2]))

  def test_select_where_gt(self):
    sql = Something.ALL.where(Something.col1 > 2)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 where s1.col1>?', [2]))

  def test_select_where_lt(self):
    sql = Something.ALL.where(Something.col1 < 2)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 where s1.col1<?', [2]))

  def test_select_where_gte(self):
    sql = Something.ALL.where(Something.col1 >= 2)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 where s1.col1>=?', [2]))

  def test_select_where_lte(self):
    sql = Something.ALL.where(Something.col1 <= 2)._sql()
    self.assertEqual(sql, ('select s1.col1,s1.col2 from something as s1 where s1.col1<=?', [2]))

  def test_insert_col_diff_name(self):
    Something2.ALL.insert(col1=1)
    self.assertEqual(self.echo.history, [('insert into something2 (col2) values (?) returning id', [1])])

  def test_insert_bad_col(self):
    with self.assertRaises(Exception):
      Something2.ALL.insert(colX=1)

  def test_where_col_diff_name(self):
    sql = Something2.ALL.where(col1=1)._sql()
    self.assertEqual(sql, ('select s1.id,s1.col2 from something2 as s1 where s1.col2=?', [1]))

  def test_order_by_col_diff_name(self):
    sql = Something2.ALL.order_by(Something2.col1)._sql()
    self.assertEqual(sql, ('select s1.id,s1.col2 from something2 as s1 order by s1.col2', []))

  def test_inner_query(self):
    sql, args = Something.ALL.where(Something.col2.in_(Something.ALL.select(Something.col1)))._sql()
    self.assertEqual(sql, 'select s1.col1,s1.col2 from something as s1 where s1.col2 in (select s1.col1 from something as s1)')
    self.assertEqual(args, [])

  def test_join(self):
    sql = A.ALL.inner_join(B, on=A.id==B.a_id)._sql()
    self.assertEqual(sql, ('select a1.id from a as a1 inner join b as b1 on a1.id=b1.a_id', []))

  def test_join_query(self):
    sql = A.ALL.left_join(B.ALL.select(B.id, B.a_id).as_('jq'), on=A.id==B.a_id.frm('jq'))._sql()
    self.assertEqual(sql, ('select a1.id from a as a1 left join (select b1.id,b1.a_id from b as b1) as jq on a1.id=jq.a_id', []))

  def test_join_query_agg(self):
    sql = A.ALL.left_join(B, on=A.id==B.a_id).select(A.id, dqo.sql.count(1)).group_by(A.id)._sql()
    self.assertEqual(sql, ('select a1.id,count(1) from a as a1 left join b as b1 on a1.id=b1.a_id group by a1.id', []))

  def test_order_by_agg(self):
    sql = A.ALL.left_join(B, on=A.id==B.a_id).select(A.id, dqo.sql.count(1)).group_by(A.id).order_by(dqo.sql.count(1).desc)._sql()
    self.assertEqual(sql, ('select a1.id,count(1) from a as a1 left join b as b1 on a1.id=b1.a_id group by a1.id order by count(1) desc', []))

if __name__ == '__main__':
    unittest.main()


