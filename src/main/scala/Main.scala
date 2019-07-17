import play.api.libs.json.{JsArray, JsObject, JsString, JsValue}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Main {

  var argMap: Map[String, String] = _

  def index(): Future[Iterator[Unit]] = {
    val OCRParser = new OCRParser()
    val S3Downloader = new S3Downloader
    val NLPParser = new NLPParser
    val ESIndexer = new ESIndexer(argMap("esurl"))
    val SQL = new SQLConn(argMap("psqlhost"), argMap("psqluser"), argMap("psqlpass"))

    var nbDone = 1
    var lastPrintLength = 0
    val nbCores = Runtime.getRuntime.availableProcessors()

    Future.traverse(SQL.getPDFAndParticipants()) { pdfPart =>
      Future{

        val participant = pdfPart(1).as[JsObject]
        val pdfs = pdfPart(0).as[Array[JsObject]]

        val parsed = pdfs.map{ pdf =>

          val text = OCRParser.parsePDF(S3Downloader.download("s3://kf-study-us-east-1-prd-sd-bhjxbdqk/", pdf("pdf_key").as[String]))
          val words = NLPParser.getLemmas(text).map(JsString)

          (participant + ("pdf_text" -> JsString(text)) + ("pdf_words" -> JsArray(words)) ++ pdf).toString()

        }

        ESIndexer.bulkIndex(parsed)

        val toPrint = s"Approx. number of participants done: ${nbDone/nbCores}"
        lastPrintLength = toPrint.length
        nbDone += 1

        println("\b"*lastPrintLength)
        println(toPrint)
      }
    }
  }

  def main(args: Array[String]) {
    argMap = {
      val defaults = Map[String, String](
        "esurl" -> "http://localhost:9200",
        "endurl" -> "limit=100&visible=true",
        "psqlhost" -> "",
        "psqlpass" -> "",
        "psqluser" -> ""
      )

      if (args.contains("--help") || args.contains("--h") || args.contains("-help") || args.contains("-h")) {
        println("Specify your arguments using this notation => arg:value.\nValues you can specify and their defaults (you must specify all empty defaults):")
        defaults.foreach( kv => println(s"""${kv._1}:${kv._2}"""))
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

      val argMap = mapFromArgsIter(args, defaults)

      argMap.values.foreach{ v =>
        if(v.equals("")) throw new Exception("You must specify all sensitive values (psqlhost, psqlpass, psqluser)")
      }

      argMap
    }

    val startTime = System.currentTimeMillis()

    Await.result(index(), Duration.Inf)
    //new SQLConn(argMap("psqlhost"), argMap("psqluser"), argMap("psqlpass")).printPDFAndParticipants()

    println("took " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
    System.exit(0)
  }
}