package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *
 * @author zengwang
 * @create 2023-03-04 19:32
 * @desc:
 */
object Y07_综合练习_7会话切割 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("Y07_综合练习_7会话切割")

    /**
     * {"account":"","appId":"cn.doitedu.app1","appVersion":"2.2","carrier":"腾讯移动","deviceId":"ne0dqfaMeYsl",
     * "deviceType":"HUAWEI-RY-10","eventId":"pageView","ip":"58.136.145.98","latitude":30.598755982100613,
     * "longitude":114.2961261186026,"netType":"3G","osName":"android","osVersion":"7.8",
     * "properties":{"pageId":"281","refUrl":"966","title":"QFk JtE WDc","url":"LjJ/OTb","utm_campain":"6",
     * "utm_loctype":"2","utm_source":"4"},"releaseChannel":"17173游戏","resolution":"1024*768",
     * "sessionId":"KHzuunyS7vF","timeStamp":1622876801395}
     */
    val rdd: RDD[String] = sc.textFile("data/app_log/input/app_log_2021-06-05.log")
    println(rdd.getNumPartitions)
    rdd.take(3).foreach(println(_))

    // json 解析
    val kvRdd: RDD[(String, JSONObject)] = rdd.map(json => {
      val obj: JSONObject = JSON.parseObject(json)
      val oldSessionId: String = obj.getString("sessionId")
      (oldSessionId, obj)
    })

    // 按照原始会话id对数据进行分组, grouped RDD 的迭代器里面的每个元素(string, iter) 又是一个迭代器
    val grouped: RDD[(String, Iterable[JSONObject])] = kvRdd.groupByKey()
    // res的找grouped 读输入，得到(string, iter). 对iter进行计算: 把iter所有数据读出来排序变成list，返回打散这个list
    // res.iter.next() = 一条打散的结果
    val res: RDD[JSONObject] = grouped.flatMap(tp => {
      val list: List[JSONObject] = tp._2.toList.sortBy(_.getLong("timeStamp"))
      // 生成一个 统计用的会话id
      var statisticSessionId: String = RandomStringUtils.randomAlphabetic(10)
      for (i <- 0 until list.size - 1) {
        list(i).put("statisticSessionId", statisticSessionId)

        if (list(i + 1).getLong("timeStamp") - list(i).getLong("timeStamp") > 60 * 30 * 1000) {
          statisticSessionId = RandomStringUtils.randomAlphabetic(10)
        }
      }
      list.last.put("statisticSessionId", statisticSessionId)
      list
    })

    // 最后保存文本的时候，会调用JSONObject 的toString方法, 对于同一个sessionId而言，保存的结果也是按照timeStamp正序的
    res.saveAsTextFile("data/app_log/output")
  }

}
