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
object RDD算子_aggregatXX {
  def main(args: Array[String]): Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.WARN)


    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("RDD算子")

    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 1), ("a", 1), ("b", 1),
                                  ("b", 1), ("c", 1), ("d", 1)), 2)
    rdd.mapPartitionsWithIndex((pIndex, iter) => {
      println(pIndex + " value " + iter.toList)

      iter
    }).collect()

    val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(10)(_ + _, _ + _)
    rdd2.foreach(println)


    val rdd3: RDD[Int] = sc.parallelize(Seq(1, 1, 1, 1, 1, 2, 2, 2, 2, 2), 3)
    val res: Int = rdd3.aggregate(100)(_ + _, _ + _)
    println(res)
    sc.stop()
  }
}
