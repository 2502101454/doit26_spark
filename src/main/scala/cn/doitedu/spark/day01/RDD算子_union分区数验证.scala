package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-02-20 14:21
 * @desc:
 */
object RDD算子_union分区数验证 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("union 分区数验证")
    val rdd: RDD[Int] = sc.parallelize(seq = 1 to 10, 2)
    rdd.coalesce(1)
    // MapPartition的算子相当于把map算子，产生新迭代器的过程完全交给用户层面实现
    // 看源码也是返回mapPartitionsRDD, 其调用了这里的function，返回一个新的iter。
    println(rdd.getNumPartitions)
    // rdd_temp 是没有数据的哈，因为迭代器的数据只可以遍历一次，这个迭代器就空掉了
    val rdd_temp: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      println(s"index ${index} iter elements ${iter.mkString(",")}")
      iter
    })
    println(rdd_temp.count())

    val rdd2: RDD[Int] = sc.parallelize(seq = 11 to 20, 3)
    println(rdd2.getNumPartitions)
    rdd2.mapPartitionsWithIndex((index, iter) => {
      println(s"index ${index} iter elements ${iter.mkString(",")}")
      iter
    }).count()

    val res: RDD[Int] = rdd.union(rdd2)
    println(res.getNumPartitions)
    res.mapPartitionsWithIndex((index, iter) => {
      println(s"index ${index} iter elements ${iter.mkString(",")}")
      iter
    }).count()

  }
}
