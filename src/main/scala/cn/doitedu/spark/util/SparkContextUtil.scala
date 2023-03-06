package cn.doitedu.spark.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author zengwang
 * @create 2023-01-31 20:34
 * @desc:
 */
object SparkContextUtil {
  def getSc(appName: String, master: String="local"):SparkContext = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)

    new SparkContext(conf)
  }
}
