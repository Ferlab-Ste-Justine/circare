/**
  * Provides implicit methods to transform case classes of strings into maps and vice-versa.
  *
  * Uses reflection. Not for performance-intensive use.
  *
  * Allows faster prototyping of CLI args, for example.
  *
  * NOTE: The case class MUST provide defaults for each of its parameters.
  */
object CaseClassUtils {
  implicit class CaseClassToMap[T <: Product](cc: T) {
    def toMap: Map[String, String] = {  //https://stackoverflow.com/questions/1226555/case-class-to-map-in-scala
      val values = cc.productIterator
      cc.getClass.getDeclaredFields.map( _.getName -> values.next.toString ).toMap
    }


    def fromMap(m: Map[String,String]): T = {
      val argList = m.foldLeft(List[String]()){ (acc, kv) =>
        val (k, v) = kv

        s"""$k="$v"""" +: acc
      }.mkString(", ")

      import scala.reflect.runtime.currentMirror
      import scala.tools.reflect.ToolBox

      val str = s""" ${cc.getClass.getCanonicalName}($argList) """

      val toolBox = currentMirror.mkToolBox()

      toolBox.compile(toolBox.parse(str))().asInstanceOf[T]
    }
  }
}

