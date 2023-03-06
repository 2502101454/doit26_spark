package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.{CombineFileInputFormat, CombineTextInputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-02-20 20:53
 * @desc:
 */
object D01_源头RDD分区数原理_小文件效率低问题 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("textFile 小文件效率低")
    val rdd: RDD[String] = sc.textFile("data/wordcount/input/a.txt")
    // sc.textFile底层就是调用sc.hadoopFile，但是InputFormat写死成了TextInputFormat
    // 我们可以自己调用sc.hadoopFile, 传入自己选择的InputFormat(CombineTextInputFormat)来解决大量小文件问题
    val rdd2: RDD[(LongWritable, Text)] = sc.hadoopFile("data/wordcount/input/a.txt", classOf[CombineTextInputFormat],
      classOf[LongWritable], classOf[Text], 2)

    val rdd3: RDD[Text] = rdd2.map(tp => tp._2)
    rdd3.foreach(println)

    sc.stop()
  }

}
