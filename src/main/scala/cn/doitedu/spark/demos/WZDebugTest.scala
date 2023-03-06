package cn.doitedu.spark.demos

/**
 *
 * @author zengwang
 * @create 2023-03-05 18:17
 * @desc:
 */
object WZDebugTest {
  def main(args: Array[String]): Unit = {

    val a = 1
    var b = 0
    if (a == 1) {
      b = 10
    } else {
      b = 3
    }

    println(b)
  }
}
