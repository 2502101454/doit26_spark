package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD


/**
 *
 * @author zengwang
 * @create 2023-02-21 20:50
 * @desc:
 */

case class Stu(id:Int, name:String, gender:String, score:Double)

class StuGenderPartitioner(numPartitions: Int) extends HashPartitioner(numPartitions) {
  override def getPartition(key: Any): Int = {
    val stu: Stu = key.asInstanceOf[Stu]
    super.getPartition(stu.gender)
  }

}


object D03_分区器的决定机制验证 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("分区器验证", "local[*]")

    val rdd: RDD[String] = sc.textFile("data/wordcount/input")
    println(rdd.partitioner) //None

    val rdd2: RDD[(String, Int)] = rdd.flatMap(s => s.split("\\s+")).map((_, 1))
    println(rdd2.partitioner) //None

    val rdd22: RDD[(String, Int)] = rdd2.repartition(3)
    println("rdd22.partitioner " + rdd22.partitioner) // None

    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    println(rdd3.partitioner) // HashPartitioner

    val rddStu = sc.parallelize(Seq(
      Stu(1, "a", "m", 90),
      Stu(2, "b", "m", 80),
      Stu(3, "v", "f", 70),
      Stu(4, "w", "m", 87),
      Stu(5, "q", "f", 98),
    ))

    println("==============")
    val repartitionRDD: RDD[Stu] = rddStu.repartition(2)
    // 非 kv 类型的rdd不会有分区器，尽管repartition shuffle了，也不会有
    println(repartitionRDD.partitioner) // None

    // 在需要partitioner的算子中，不传入分区器，则算子底层生成默认的分区器(但不全是HashPartitioner)
    val rddGrouped1: RDD[(String, Iterable[Stu])] = rddStu.groupBy(stu => stu.gender)
    println(rddGrouped1.partitioner) // HashPartitioner

    val rddGrouped2: RDD[(Stu, Iterable[Stu])] = rddStu.groupBy(stu => stu, new StuGenderPartitioner(4))
    println(rddGrouped2.partitioner) // StuGenderPartitioner

    // 源码里是先转(k, v) rdd，再调SortByKey() 此时有分区器，但是接着调values，得到MapPartitionsRDD，分区器就丢了
    val rdd4: RDD[Stu] = rddStu.sortBy(stu => stu.score)
    println(rdd4.partitioner) // None

    val rdd5: RDD[(Double, Stu)] = rdd4.map(stu => (stu.score, stu)).sortByKey(true)
    println(rdd5.partitioner) // RangePartitioner
  }
}
