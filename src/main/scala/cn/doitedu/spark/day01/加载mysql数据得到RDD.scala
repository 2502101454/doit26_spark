package cn.doitedu.spark.day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{DriverManager, ResultSet}

case class Soldier(id: Int, name: String, role: String, battle: Double)
/**
 *
 * @author zengwang
 * @create 2023-01-29 13:22
 * @desc:
 */
object 加载mysql数据得到RDD {
  def main(args: Array[String]): Unit = {
    // 这些用`.`分割的就是日志记录器的父子关系，org就是顶级父了
    val logger: Logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.WARN)


    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("MysqlRDD_Demo")

    val sc = new SparkContext(conf)

    // 创建一个函数，返回数据库连接
    val getConn = () => {
      DriverManager.getConnection("jdbc:mysql://localhost:3306/wz_test", "root", "12345678")
    }

    val sql = "select * from soldiers where id >= ? and id <= ?"
    /*
    查看JdbcRDD的参数文档：
    1.sql语法要求不能全表查询，必须指定过滤条件
    2.这个过滤条件将被用于分区，比如要求0 < id < 100的记录，如果现在两个分区的话，则会有两个task，
    对数据根据过滤条件进行均分，一个task读0-50, 一个读51-100，会对mysql发起两个请求

    分区的哲学：分而治之数据
     */

    // 将底层通过JDBC获取的数据，和样例对象进行映射，RDD的数据类型就是样例对象
    val resMapping = (res: ResultSet) => {
      val id: Int = res.getInt(1)
      val name: String = res.getString(2)
      val role: String = res.getString(3)
      val battle: Float = res.getFloat(4)

      Soldier(id = id, name = name, role = role, battle = battle)
    }
    val rdd = new JdbcRDD[Soldier](sc, getConn, sql,
      1, 100, 2, resMapping)
    rdd.foreach(println)
    sc.stop()
  }


}
