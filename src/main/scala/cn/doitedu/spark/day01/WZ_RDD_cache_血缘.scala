package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-03-07 23:14
 * @desc:
 */
object WZ_RDD_cache_血缘 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("WZ_RDD_cache_血缘")
    val rdd: RDD[Int] = sc.parallelize(1 to 1000, 2)
    val rdd2: RDD[(Int, String)] = rdd.map((_, "x"))
    rdd2.persist()
    val rdd_a: RDD[(Int, String)] = rdd2.filter(_._1 % 2 == 0)
    val rdd_aa: RDD[(Int, Iterable[String])] = rdd_a.groupByKey(3)
    rdd_aa.count()

    val rdd_b: RDD[(Int, String)] = rdd2.filter(_._1 % 2 != 0)
    rdd2.unpersist()
    val rdd_bb: RDD[(Int, String)] = rdd_b.reduceByKey(_ + _)
    rdd_bb.count()


    Thread.sleep(Long.MaxValue)
    sc.stop()

  }

}
