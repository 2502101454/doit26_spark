package cn.doitedu.spark.deepin

import java.io.{BufferedReader, File, FileReader}
import java.util.Scanner

/**
 *
 * @author zengwang
 * @create 2023-02-06 20:00
 * @desc:
 */
class MyFileIterator(filePath:String) extends Iterator[String]{
  private val br = new BufferedReader(new FileReader(filePath))

  var line: String = null

  // 先调用hasNext，返回true 就调用next
  override def hasNext: Boolean = {
    line = br.readLine()
    line != null
  }

  override def next(): String = {
    line
  }
}

class MyFileIterator2(filePath:String) extends Iterator[String] {
  private val scanner = new Scanner(new File(filePath))
  override def hasNext: Boolean = {
    scanner.hasNextLine
  }

  override def next(): String = {
    scanner.next()
  }
}

// 可迭代对象 是对迭代器的在封装，提供了更多的算子 API
class MyFileIterable(filePath:String) extends Iterable[String] {
  override def iterator: Iterator[String] = new MyFileIterator(filePath)
}
// 使用可迭代对象实现wordCount
object MyFileIteratorTest {
  def main(args: Array[String]): Unit = {
    val iterable = new MyFileIterable("data/wordcount/input/a.txt")
    val res: Map[String, Int] = iterable.flatMap(line => line.split("\\s+")).map(s => (s, 1))
      .groupBy(_._1).mapValues(iter => iter.size)
    res.foreach(println)
  }
}
