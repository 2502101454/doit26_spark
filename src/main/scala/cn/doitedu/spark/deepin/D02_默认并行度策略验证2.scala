package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-02-21 12:53
 * @desc:
 */
object D02_默认并行度策略验证2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("D02_默认并行度策略验证2")
    sparkConf.setMaster("local[4]")
//    sparkConf.set("spark.default.parallelism", "3")

    val sc = new SparkContext(sparkConf)
    /***
     * A.默认分区数机制，下的worker线程数:
     *  1.设置分区参数spark.default.parallelism = 3
     *  master=local时，1 < 3，最终只有1个worker线程；
     *  master=local[4]时，4 > 3，最终只有3个worker线程;
     *  2.不设置spark.default.parallelism
     *  P: 读取total_cores = master, N 总是= P，启动N个线程，有N个分区
     * B.用户自己设置算子分区
     *  N P理论依然成立
     *
     * 总结： 单独求P，单独求N 互不影响
     */
    val rdd: RDD[Int] = sc.parallelize(seq = 1 to 10000000)
    println(rdd.getNumPartitions) // 2
    // rdd 和rdd2 分区器不同
    println("repartition for 9")
    val rdd2: RDD[Int] = rdd.repartition(9)
    // rdd2 和rdd3 分区器相同
    println("repartition for 2")
    val rdd3: RDD[Int] = rdd2.repartition(2)
    println("repartition for 5")
    val rdd4: RDD[Int] = rdd3.repartition(5)
    rdd4.count()
    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
