package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.antlr.v4.runtime.tree.xpath.XPathRuleAnywhereElement
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-02-28 19:04
 * @desc:
 */
object D04_shuffle算子不一定产生shuffle {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc(appName="shuffle算子不一定产生shuffle", master = "local")
    // 走defaultParallelism逻辑，默认分区数是worker线程数， 分区数也是2
    val rdd1: RDD[(String, Int)] = sc.parallelize(Seq(
      ("a", 1),
      ("b", 1),
      ("c", 1),
      ("a", 1),
      ("x", 1),
      ("d", 1),
    ))

    val rdd10: RDD[(String, Int)] = sc.parallelize(Seq(
      ("a", 1),
      ("b", 1),
      ("d", 1),
      ("w", 1),
      ("c", 1),
      ("d", 1),
    ))
    println(s"rdd1.partitioner ${rdd1.partitioner}") // None
    // rdd1没有分区器，partitionBy算子自己传入新的分区器，不相等，因此源码走shuffleRDD
    val rdd2: RDD[(String, Int)] = rdd1.partitionBy(new HashPartitioner(3))
    println(s"rdd2.partitioner ${rdd2.partitioner}") // Some(org.apache.spark.HashPartitioner@3)
    // reduceByKey自己启用了 默认分区器的机制：使用父rdd2的分区器
    // 所以，rdd3和rdd2的分区数一样，不shuffle，创建MapPartitionsRDD，但是preservesPartitioning=true，即保留rdd的分区器
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    println(s"rdd3.partitioner ${rdd3.partitioner}") // Some(org.apache.spark.HashPartitioner@3)
    rdd3.count()

    // 里面走shuffle，但源码后面values了一下，返回MapPartitionsRDD，把分区器丢了
    val rdd4: RDD[(String, Int)] = rdd1.repartition(2)
    // reduceByKey算子内部 默认分区器的机制：自己创建了HashPartitioner(2)； 和rdd4的分区器不相等，创建shuffleRDD，走shuffle
    val rdd5: RDD[(String, Int)] = rdd4.reduceByKey(_ + _)
    println(s"rdd5.partitioner ${rdd5.partitioner}") // Some(org.apache.spark.HashPartitioner@2)
    rdd5.count()


    /**
     * Join算子不一定产生Shuffle
     * 之前宽依赖里面有个 join co-partitioned，没有Shuffle的
     * 思路是：rdd3 = rdd1.join(rdd2)
     * 如果rdd1 、rdd2、rdd3的分区器都一样，就不用Shuffle
     */

    val joined1: RDD[(String, (Int, Int))] = rdd1.join(rdd10)
    println(s"joined1.partitioner ${joined1.partitioner}") // Some(org.apache.spark.HashPartitioner@1)
    // 有Shuffle，因为父rdd都没有分区器，子rdd有分区器，它们的分区规则不一样
    joined1.count()

    val rdd_a: RDD[(String, Int)] = rdd1.partitionBy(new HashPartitioner(2))
    val rdd_b: RDD[(String, Int)] = rdd10.partitionBy(new HashPartitioner(2))
    val joined2: RDD[(String, (Int, Int))] = rdd_a.join(rdd_b)
    // 没有Shuffle, 因为子rdd的分区器就是沿用了父rdd的最大分区器
    joined2.count()
    // 依然没有Shuffle
    val joined3: RDD[(String, (Int, Int))] = rdd_a.join(rdd_b, new HashPartitioner(2))
    joined3.count()

    val joined4: RDD[(String, (Int, Int))] = rdd_a.join(rdd_b, new HashPartitioner(3))
    joined4.count() // 有Shuffle，因为子rdd的分区器和数 != 父RDD的分区器和数

    Thread.sleep(Long.MaxValue)
    sc.stop()

  }
}
