package cn.doitedu.spark.day01

/**
 *
 * @author zengwang
 * @create 2023-02-01 22:51
 * @desc:
 */
object Scala闭包 {
  def main(args: Array[String]): Unit = {
    var a = 100
    val f = (i: Int) => {
      var res = i + a +10
      // 闭包体里面可以修改外部变量的值
      a = 300
      res
    }

    println(f(10)) // 120
    println(a) // 300

    println(f(10)) // 320
  }
}
