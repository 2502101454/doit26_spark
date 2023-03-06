package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-02-16 20:06
 * @desc:
 */
object Dependency生成验证 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc(appName = "Dependency生成验证")
    val rdd: RDD[String] = sc.parallelize(Seq("a a a b c d", "s c c a b d"))
    // rdd2 (MapPartitioinsRDD) 窄依赖于rdd
    val rdd2: RDD[String] = rdd.flatMap(s => s.split("\\s+"))
    // rdd3 (MapPartitionsRDD) 窄依赖于rdd2
    val rdd3: RDD[(String, Int)] = rdd2.map(w => (w, 1))

    // rdd4 (ShuffledRDD) 宽依赖于rdd3
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _, 2)

    rdd4.saveAsTextFile("data/wordcount/output")

    // 主线程不终止，可以观察spark的WEB UI，日志被我关了(http://localhost:4040/jobs/)
    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
