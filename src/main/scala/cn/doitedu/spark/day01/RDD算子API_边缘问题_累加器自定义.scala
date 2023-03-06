
package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import scala.collection.mutable

/**
 *
 * @author zengwang
 * @create 2023-02-02 19:58
 * @desc:
 */
object RDD算子API_边缘问题_累加器自定义 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("累加器", "local[3]")

    /**
     * 需求： 给你一堆脏数据，让你统计每个城市的人数，以及处理过程中的脏数据类型及条数
     * 思路1: 用多个累加器，分别对应各种异常类型，用于统计其条数
     */
    val info: RDD[String] = sc.parallelize(Seq(
      "1,Mr.duan,18,beijing",
      "2,Mr.zhao,19,shanghai",
      "3,Mr.wang,24",
      "4,Mr.liu,20,beijing",
      "a,Mr.ma,18,dezhou",
      "b,Mr.duan,30,shenzhen"
    ))

    println("partitions num:" + info.getNumPartitions)
    val field_not_enough: LongAccumulator = sc.longAccumulator("field_not_enough")
    val id_malformed: LongAccumulator = sc.longAccumulator("id_malformed")
    // 自定义的累加器使用，需要先注册
    val myAccumulator = new CustomAccumulator(new mutable.HashMap[String, Long]())
    sc.register(myAccumulator)

    val tuples: RDD[(Int, String, Int, String)] = info.map(line => {
      var res:(Int, String, Int, String) = null

      try {
        val arr: Array[String] = line.split(",")
        val id: Int = arr(0).toInt
        val name: String = arr(1)
        val age: Int = arr(2).toInt
        val city: String = arr(3)
        res = (id, name, age, city)
      } catch {
        case e: ArrayIndexOutOfBoundsException => {
          field_not_enough.add(1)
          myAccumulator.add(("ArrayIndexOutOfBoundsException", 1))
        }
        case e: NumberFormatException => {
          id_malformed.add(1)
          myAccumulator.add(("NumberFormatException", 1))
        }
        case _ => myAccumulator.add(("other", 1))
      }
      res
    })

    // 按照city分组
    val res: RDD[(String, Int)] = tuples.filter(_ != null).groupBy(_._4)
      .map(tp => {
        // key是city，value是迭代器
        (tp._1, tp._2.size)
      })

    res.foreach(println)
    // 查看累加器的值
    println(field_not_enough.value)
    println(id_malformed.value)
    println(myAccumulator.value)

    sc.stop()
  }
}

/**
 * IN 泛型: 要在累加器上调add()时，插入的参数类型
 * OUT泛型: 在累加器上调value() 所返回的数据类型
 */
class  CustomAccumulator(valueMap: scala.collection.mutable.HashMap[String, Long]) extends AccumulatorV2[(String, Int),
    scala.collection.mutable.HashMap[String, Long]] {
  // 判断累加器是否为初始状态
  override def isZero: Boolean = this.valueMap.isEmpty

  // 拷贝一个累加器对象的方法，executor端每个task都会执行copy，从而获取累加器的副本
  override def copy(): AccumulatorV2[(String, Int), scala.collection.mutable.HashMap[String, Long]] = {

    new CustomAccumulator(new mutable.HashMap[String, Long]())
  }

  // 将累加器重置
  override def reset(): Unit = this.valueMap.clear()

  // 对累加器更新值
  override def add(v: (String, Int)): Unit = {
    val past: Long = this.valueMap.getOrElse(v._1, 0)
    val now: Long = past + v._2
    this.valueMap.put(v._1, now)
  }

  // driver端需要对各个task所产生的累加器进行结果合并
  override def merge(other: AccumulatorV2[(String, Int), scala.collection.mutable.HashMap[String, Long]]): Unit = {
    val otherValueMap: mutable.HashMap[String, Long] = other.value
    otherValueMap.foreach(tp => {
      val past: Long = this.valueMap.getOrElse(tp._1, 0)
      val now: Long = otherValueMap(tp._1) + past
      this.valueMap.put(tp._1,  now)
    })
  }

  // 从累加器上取值
  override def value: scala.collection.mutable.HashMap[String, Long] = this.valueMap
}