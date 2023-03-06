package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-02-21 12:53
 * @desc:
 */
object D02_默认并行度策略验证 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("验证默认并行度", master = "local[*]") // 2
    // 先前local[40]，分区数20, 表达想要创建40个线程，if 40 > 20，则创建20个线程，因为创建再多也是浪费
    // local[40], 分区数 36 ，创建36个线程
    // 先前local[30]，分区数取默认，也是p=30，因此创建了30个线程
    // 小于呢，local[30] 、 分区数36，则创建30个线程
    // 小于呢，local[5] 、 分区数36，则创建5 worker个线程

    /*
     我们常说的: 分区数等于task数，是对的，但一个task是不等于一个线程，task数和线程数没关系
     spark executor运行task时，会先创建线程池，线程池中的worker线程数量是如何决定的呢?

     一、本地模式:
     1.优先取master传入的local[N]，比如这里代表了你想要创建N个worker线程
     worker线程的个数和CPU逻辑核数没有关系，CPU逻辑核数只是说明了最大的并行线程数。
     假设你的CPU是16核数，你都可以写成local[30] spark考虑创建30个worker线程; local[5]则考虑创建5个worker线程


     2.其次判断worker线程数N 和分区数P的关系
     如果N<=P，最后创建N个线程；如果N > P，最后只创建P个线程，多了也是资源浪费

     二、集群模式
     1.一个executor启动后，executor-cores是代表worker线程池中的线程数
     比如一个executor，它的cores是4，则会启动4个线程，现在给它来了16个分区task，则每个线程会轮流执行4个task

     三、算子默认分区数逻辑，只是为了表达出你的task个数（分区只是数据逻辑的划分）

     如上，有了线程数、task数，那么worker线程如何执行task们呢？
     task数 > worker线程数时：
     `轮流`执行：worker先执行完手头的task再执行别的task，而不是在多个task之间来回切换执行！！

     worker线程能否并行运行取决于cpu核数，运行的只是worker线程，task只是静态概念
     比如本地举例，16核，2个worker线程并行运行，因此可以同时处理2个task，处理完手头task后再处理别的task，截图
     个人理解：cpu16核，20个worker线程，同一时刻最大运行16个线程(即处理16个task), 但是cpu太快了，另外个线程对应的4个task也会执行的
     16核切换执行20个线程，每个线程获得资源调度，执行的时候，对手里的task都不松手的，直到task执行完了才换新的task继续执行

     spark UI中只是给出来worker线程和Task执行的效果图，并没有给出cpu切换worker线程的效果图 */



    // 没有传入分区参数，底层走默认的并行度计算策略来得到默认的分区数
    // SchedulerBackend.defaultParallelism()

    // local 默认运行: scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // 背景: local[2]，100个分区，100个task，每个task中描述100个元素，每个元素处理的时候要sleep20ms，
    // 则一个task一共sleep 2秒，本地模式2个worker线程，一次执行2个task，执行完手头的task后再取未执行的task
    val rdd: RDD[Int] = sc.parallelize(1 to 10000, 100)
    println(rdd.getNumPartitions)
    rdd.map(s => {
      Thread.sleep(20)
      (s, 1)
    }).reduceByKey(_ + _, 40).count()
    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
