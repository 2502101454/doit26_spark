package cn.doitedu.spark.deepin

import cn.doitedu.spark.day01.Soldier

import java.sql.{Connection, DriverManager, ResultSet, Statement}

/**
 *
 * @author zengwang
 * @create 2023-02-06 20:33
 * @desc:
 */
class MySqlTableIterator (url:String, username:String, password:String, table:String) extends Iterator[Soldier]{

  private val conn: Connection = DriverManager.getConnection(url, username, password)
  private val stmt: Statement = conn.createStatement()
  private val resultSet: ResultSet = stmt.executeQuery(s"select * from ${table}")

  override def hasNext: Boolean = {
    // 将游标移到下一行，返回true or false
    resultSet.next()
  }

  override def next(): Soldier = {
    // 读取一行记录
    val id: Int = resultSet.getInt("id")
    val name: String = resultSet.getString("name")
    val role: String = resultSet.getString("role")
    val battle: Double = resultSet.getDouble("battle")
    // 这里可以理解为本迭代器自己的运算
    Soldier(id, name, role, battle)
  }
}

class MySqlTableIterable(url:String, username:String, password:String, table:String) extends Iterable[Soldier] {
  override def iterator: Iterator[Soldier] = new MySqlTableIterator(url, username, password, table)
}

// 通过可迭代对象 统计，每个角色的总战力
object MySqlTableIterableTest{
  def main(args: Array[String]): Unit = {
    val iterable = new MySqlTableIterable("jdbc:mysql://localhost:3306/wz_test", "root",
      "12345678", "soldiers")

    // 迭代器 产生的元素，元素中包含了一个子迭代器, trait Map[K, +V] extends Iterable[(K, V)]
    val groupedIter: Map[String, Iterable[Soldier]] = iterable.groupBy(soldier => soldier.role)
    val res: Map[String, Double] = groupedIter.mapValues(iter => {
      iter.map(_.battle).sum
    })
    res.foreach(println)
  }
}

object MySqlTableIteratorTest{
  def main(args: Array[String]): Unit = {
    val iter = new MySqlTableIterator("jdbc:mysql://localhost:3306/wz_test", "root",
      "12345678", "soldiers")
    iter.foreach(println)
  }
}
