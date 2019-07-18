import java.io.File
import java.nio.file.{Files, Paths}

import scala.collection.immutable

object Walker {
  def walk(dir: String, filter: File => Boolean = f => f.getName.toLowerCase().endsWith(".pdf")): immutable.Seq[File] = {
    new File(dir).listFiles().foldLeft(List[File]()) { (acc, file) =>
      if(filter(file)) file +: acc
      else acc
    }
  }
}
