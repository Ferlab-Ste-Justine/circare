import scala.annotation.tailrec

object Main {

  var argMap: Map[String, String] = _

  def index() = {

  }

  def main(args: Array[String]) {
    argMap = {
      val defaults = Map[String, String](
        "esurl" -> "http://localhost:9200",
        "singleindex" -> "",
        "languages" -> "eng",
      )

      if (args.contains("--help") || args.contains("--h") || args.contains("-help") || args.contains("-h")) {
        println("Specify your arguments using this notation => arg:value.")
        println("Values you can specify and their defaults:")
        defaults.foreach( kv => println(s"\t${kv._1}:${kv._2}"))
        println("If singleindex is empty, a new index will be created for every subfolder in the directory.")
        println("The input folder can contain other folders. However, if it does, it can only contain folders, and those folders can only contain PDFs.")
        println("Otherwise, if the input folder only contains PDFs, indexation will be started with the input folder as the index name (or singleindex, if specified)")
        System.exit(0)
      }

      @tailrec
      def mapFromArgsIter(argList: Array[String], argMap: Map[String, String]): Map[String, String] = argList match {
        case Array() => argMap
        case Array(h, _*) =>
          val firstColon =  h.indexOf(":")
          val key = h.substring(0, firstColon)
          val value = h.substring(firstColon+1, h.length)

          if(!defaults.contains(key)) throw new Exception(s"Unrecognized argument: $key")

          mapFromArgsIter(argList.tail, argMap + (key -> value))
      }

      mapFromArgsIter(args, defaults)
    }

    val startTime = System.currentTimeMillis()

    val OCRParser = new OCRParser(argMap("languages"))
    Walker.walk("input").foreach{ pdfFile =>
      println(OCRParser.parsePDF(pdfFile))
    }
    //Await.result(index(), Duration.Inf)
    //new SQLConn(argMap("psqlhost"), argMap("psqluser"), argMap("psqlpass")).printPDFAndParticipants()

    println("took " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
    System.exit(0)
  }
}