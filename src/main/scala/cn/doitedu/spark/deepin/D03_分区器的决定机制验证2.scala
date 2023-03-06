package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-02-21 20:50
 * @desc:
 */

object D03_分区器的决定机制验证2 {
  def main(args: Array[String]): Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setAppName("分区数验证2")
    conf.setMaster("local")
    conf.set("spark.default.parallelism", "4")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/wordcount/input")
    println(rdd.partitioner) //None

    val rdd2: RDD[(String, Int)] = rdd.flatMap(s => s.split("\\s+")).map((_, 1))
    println(rdd2.partitioner) // None
    println(rdd2.getNumPartitions) // 2

    // 验证普通的reduceByKey后，子rdd默认分区数
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    println(s"rdd3.getNumPartitions ${rdd3.getNumPartitions}") // 2
    println(s"rdd3.partitioner ${rdd3.partitioner}") // Some(org.apache.spark.HashPartitioner@2)

    // 验证用户设置了并行度后的reduceByKey后，子rdd的分区数为4, 分区器 Some(org.apache.spark.HashPartitioner@4)

    // 验证有分区器的情况，父rdd：最大分区器的分区数是4，最大分区数是4
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _)
    println(s"rdd4.partitioner ${rdd4.partitioner}") // Some(org.apache.spark.HashPartitioner@4)

    // 验证多个rdd join后 子rdd的并行度
    val rdd5: RDD[(String, Int)] = sc.parallelize(seq = 1 to 1000000, 1000).map(s => (s.asInstanceOf[String], 1))
    println(s"rdd5.partitioner ${rdd5.partitioner}") // None
    println(rdd5.getNumPartitions) // 1000

    // 验证无分区器 和 有分区器的rdd join, 在用户设置分区数的情况下,尽快最大分区数超过了最大分区器的分区数的10倍
    val rdd6: RDD[(String, (Int, Int))] = rdd5.join(rdd4)
    println(s"rdd6.partitioner${rdd6.partitioner}") // 使用最大分区器rdd的分区器 Some(org.apache.spark.HashPartitioner@4)

    // 提前注掉用户设置的分区数:
    // 验证无分区器 和 有分区器的rdd join，在用户未设置分区数的情况下
    val rdd7: RDD[(String, (Int, Int))] = rdd5.join(rdd4) // rdd4变成 Some(org.apache.spark.HashPartitioner@2)
    println(s"rdd7.partitioner ${rdd7.partitioner}") // Some(org.apache.spark.HashPartitioner@3e8)
    println(s"rdd7.getNumPartitions ${rdd7.getNumPartitions}") // 1000


    val rdd8: RDD[(String, Int)] = sc.parallelize(Seq(
      ("a", 1),
      ("b", 1),
      ("c", 1),
      ("a", 1),
      ("c", 1),
    ))
    println(s"rdd8.partitioner ${rdd8.partitioner}") // None
    println(s"rdd8.getNumPartitions ${rdd8.getNumPartitions}") // 4

    val rdd9: RDD[(String, Int)] = rdd8.reduceByKey(_ + _, 3)
    println(s"rdd9.partitioner ${rdd9.partitioner}") //  Some(org.apache.spark.HashPartitioner@3)
    // (a, 2) (c, 2) (b, 1)
    val rdd10: RDD[(String, Int)] = rdd9.reduceByKey(_ + _)
    println(s"rdd10.partitioner ${rdd10.partitioner}") //  Some(org.apache.spark.HashPartitioner@3)
  }
}
