package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-02-19 20:33
 * @desc:
 */
object RDD算子API_边缘问题_checkpoint {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("checkpoint演示")
    sc.setCheckpointDir("data/checkpoint")

    val rdd: RDD[Int] = sc.parallelize(1 to 1000)
    val rdd2: RDD[(Int, Int)] = rdd.map(i => (i % 100, i))
    val rdd3: RDD[(Int, Int)] = rdd2.map(tp => (tp._1, tp._2 * 10))

    rdd3.cache()
    // 还可以把rdd3持久化HDFS
    /**
     * 如果你觉得你这个rdd3十分宝贵，一旦丢失后重新计算，代价太大(原始数据集很大、逻辑链条复杂)，
     * 就可以对rdd3进行checkpoint;
     *
     * 如果程序崩了，不用你手动重启，spark内部会重试
     */
    rdd3.checkpoint() // 会额外启动一个job来计算rdd3的数据并存到HDFS

    rdd3.reduceByKey(_ + _).count() //job1
    rdd3.map(tp => (tp._1 + "a", Math.min(tp._2, 1000))).groupByKey().mapValues(iter => iter.max).count() // job2

    Thread.sleep(Long.MaxValue)
    sc.stop()

  }
}
