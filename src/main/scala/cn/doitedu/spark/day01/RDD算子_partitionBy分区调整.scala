package cn.doitedu.spark.day01

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-01-31 20:49
 * @desc:
 */

object RDD算子_分区调整补充 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("RDD行动算子")
    val rdd: RDD[(Int, Char)] = sc.parallelize(Seq((1, 'a'), (3, 'b'), (5, 'd'), (2, 'c'), (4, 'w')), 2)
    // HashPartitioner 是将key的hashcode % 分区数，来决定一条数据属于哪个分区，下面是变成3个分区
    val rdd2: RDD[(Int, Char)] = rdd.partitionBy(new HashPartitioner(3))
    println(rdd2.getNumPartitions)

    // 对于key是复杂类型的 rdd，也可以自定义分区规则: Person(id, age, salary), Job
    val kvRDD: RDD[(Person, String)] = sc.parallelize(Seq((Person(3, 28, 9000), "dev"), (Person(2, 30, 29000), "sale"),
                                                     (Person(1, 29, 40000), "sale"), (Person(4, 45, 7000), "dev"),
                                                     (Person(0, 15, 5000), "security-guards")), 2)
    kvRDD.partitionBy(new OrderPartition(3))
    // 也可以使用匿名内部类的写法

  }
}

class OrderPartition(partitions: Int) extends HashPartitioner(partitions: Int) {
  override def numPartitions: Int = super.numPartitions

  // 实现自己的分区规则
  override def getPartition(key: Any): Int = {
    val person: Person = key.asInstanceOf[Person]
    // 把age取出来，调用父类的分区逻辑(age.hashcode() % numPartitions), hashcode()可以为负数，
    super.getPartition(person.age)
  }
}