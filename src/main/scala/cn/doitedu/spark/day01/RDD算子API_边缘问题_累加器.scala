package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 *
 * @author zengwang
 * @create 2023-02-02 19:58
 * @desc:
 */
object RDD算子API_边缘问题_累加器 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("累加器", "local[*]")
    val rdd: RDD[String] = sc.textFile(path="data/wordcount/input/a.txt", minPartitions = 3)
    val rdd2: RDD[(String, Int)] = rdd.flatMap(line => {
      val word: Array[String] = line.split(" ")
      word.map(new Tuple2[String, Int](_, 1))
    })

    // 需求：统计一共多少个单词，并且输出rdd2的每个元素
    // collect算子，一次job
    // println(rdd2.collect().length)
    // foreach是action算子，一次job
    // rdd2.foreach(println)

    val accumulator: LongAccumulator = sc.longAccumulator("word_nums")
    rdd2.foreach(line => {
      println(line)
      accumulator.add(1)
    })
    println(accumulator.value)
  }
}
