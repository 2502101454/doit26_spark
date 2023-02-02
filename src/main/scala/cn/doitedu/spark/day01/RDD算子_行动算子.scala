package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-01-31 20:49
 * @desc:
 */
case class Person(id: Int, age: Int, salary: Int)

object RDD算子_行动算子 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("RDD行动算子")
    val rdd: RDD[Person] = sc.parallelize(Seq(Person(3, 28, 9000), Person(2, 30, 29000),
                      Person(1, 29, 40000), Person(4, 45, 7000)), 2)

    implicit val ord: Ordering[Person] = new Ordering[Person] {
      override def compare(x: Person, y: Person): Int = x.age.compare(y.age)
    }
    // 按照用户年龄 正序，取全局top2
    val persons: Array[Person] = rdd.takeOrdered(2)
    persons.map(println)

  }
}
