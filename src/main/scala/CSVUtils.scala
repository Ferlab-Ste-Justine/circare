import play.api.libs.json.JsObject

import scala.collection.Iterator.empty
import scala.io.BufferedSource

object CSVUtils {

  case class Close[A](iterator: Iterator[A], source: BufferedSource) extends Iterator[A] {
    override def foldLeft[B](z: B)(op: (B, A) => B): B = {
      val b = iterator.foldLeft(z)(op)
      source.close()
      b
    }

    override def hasNext: Boolean = {
      val isNonEmpty = iterator.hasNext
      if(isNonEmpty) true
      else {
        println("heyo!")
        false
      }
    }

    override def next(): A = iterator.next()
  }

  def readCSV(path: String, sep: String): Iterator[Array[String]] = {
    val bufferedSource: BufferedSource = io.Source.fromFile(path)

    val lines: Iterator[Array[String]] = bufferedSource.getLines().map(_.split(sep))

    CSVUtils.Close(lines, bufferedSource)
  }

  def readCSVSkip(path: String, sep: String): Iterator[Array[String]] = {
    val i = CSVUtils.readCSV(path, sep)
    i.next()
    i
  }

  def jsonToCSV(json: JsObject, fields: List[String], sep: String): String = ???

  def jsonToCSV(json: List[JsObject]): String = ???
}
