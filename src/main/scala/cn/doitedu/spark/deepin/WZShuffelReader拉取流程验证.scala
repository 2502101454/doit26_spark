package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-03-05 17:47
 * @desc:
 */
object WZShuffelReader拉取流程验证 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("WZShuffelReader拉取流程验证", master = "local[1]")

    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(
      ("a", 1),
      ("b", 1),
      ("d", 1),
      ("w", 1),
      ("c", 1),
      ("d", 1),
      ("a", 1),
      ("b", 1)
    ))

//    val rdd2: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
//    rdd2.count()

    val rdd3: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    rdd3.count()
  }

}
