package cn.doitedu.spark.deepin

/**
 *
 * @author zengwang
 * @create 2023-02-06 19:58
 * @desc:
 */
object IteratorTest {
  def main(args: Array[String]): Unit = {
    val l = List(1, 2, 3, 4, 5)
    val iter: Iterator[Int] = l.iterator
    // 迭代器只能被遍历一次,就变成空的了
    println(iter.mkString(","))
//    println(iter.sum)
//    println(iter.max)

    /**
     * scala 迭代器的lazy特性
     * 相当于rdd上的transformation算子
     */
    val iter2: Iterator[Int] = iter.map(e => {
      println("f1被执行了")
      e + 10
    })

    println(iter2.max)
    return
    val iter3: Iterator[Int] = iter2.map(e => {
      println("f2被执行了")
      e * 10
    })

    /**
     * iter3:
     *  hasNext = iter2.hasNext = iter.hasNext
     *  next() = f2(iter2.next())
     *          iter2.next() = f1(iter.next())
     * 所以 iter3.next() = f2(f1(iter.next()))
     * 流程：从数据源，取一条数据，然后执行逻辑链条，得到结果
     */

    // 相当于rdd中的actions算子
    iter3.foreach(println)
  }
}
