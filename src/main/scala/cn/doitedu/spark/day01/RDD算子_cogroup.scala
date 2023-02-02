package cn.doitedu.spark.day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author zengwang
 * @create 2023-01-31 12:57
 * @desc:
 */
object RDD算子_cogroup {
  def main(args: Array[String]): Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("RDD算子")

    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("d", 0)), 2)
    val rdd2: RDD[(String, Char)] = sc.parallelize(Seq(("a", 'x'), ("a", 'w'), ("c", 'p'), ("d", 'k')), 3)
    // join后的下游rdd是4个分区，这个4个分区描述了从前面两个rdd的分区转变来的逻辑
    val value: RDD[(String, (Iterable[Int], Iterable[Char]))] = rdd.cogroup(rdd2, 4)
    value.foreach(println)
    sc.stop()
  }
}
