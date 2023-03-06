package cn.doitedu.spark.deepin

/**
 *
 * @author zengwang
 * @create 2023-02-07 19:40
 * @desc:
 */
object WZTest {
  def main(args: Array[String]): Unit = {

    val iter = new MyFileIterator2("data/wordcount/input/a.txt")
    var tmp = ""
    while (iter.hasNext) {
      tmp = iter.next()
    }
//    val words: Iterator[String] = iter.flatMap(line => line.split("\\s+"))
//    var word = ""
//    while (words.hasNext) {
//      word = words.next()
//    }
  }
}
