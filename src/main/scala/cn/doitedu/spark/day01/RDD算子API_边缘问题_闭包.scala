package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 *
 * @author zengwang
 * @create 2023-02-01 20:33
 * @desc:
 */
object RDD算子API_边缘问题_闭包 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("RDD算子API边缘问题")
    val rdd: RDD[(Int, String)] = sc.parallelize(Seq((1, "北京"), (2, "上海")))

    // 普通对象，在Driver程序中创建的
    var mp = new mutable.HashMap[Int, String]()
    mp.put(1, "张三")
    mp.put(2, "李四")

    // object对象，在Driver端创建
    val data = JobName

    val rdd2: RDD[(Int, String, String, String)] = rdd.map(tuple => {
      // 普通对象，每个task中都独自持有一份，不存在任何线程安全问题
      val name: String = mp.get(tuple._1).get

      // 单例对象 只在每个executor中存在一个，由executor中的多个task线程共享
      // 不要在这里对改变了进行修改，否则会产生线程安全问题
      val job: String = data.job

      (tuple._1, name, data.job, tuple._2)
    })
    rdd2.foreach(println)



  }
}

object JobName extends Serializable {
  val job: String = "开发"
}
