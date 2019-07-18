import java.io.File

import play.api.libs.json.{JsArray, JsObject, JsString, JsValue, Json}

import scala.annotation.tailrec
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object Main {

  case class ArgMap(esurl: String = "http://localhost:9200", singleindex: String = "", languages: String = "eng", sep: String = ",")

  var argMap: ArgMap = _

  def main(args: Array[String]) {
    argMap = {

      val defaultsMap = Map("esurl" -> "http://localhost:9200", "singleindex" -> "", "languages" -> "eng", "sep" -> ",")

      println(s"You entered these arguments: ${args.mkString(" ")}")

      if (args.contains("--help") || args.contains("--h") || args.contains("-help") || args.contains("-h")) {
        println("Specify your arguments using this notation => arg:value.")
        println("Values you can specify and their defaults:")
        defaultsMap.foreach{ case (k, v) => println(s"\t$k:$v") }
        println("If singleindex is empty, a new index will be created for every subfolder in the directory.")
        println("The input folder can contain other folders. However, if it does, it can only contain folders, and those folders can only contain PDFs.")
        println("Otherwise, if the input folder only contains PDFs, indexation will be started with the input folder as the index name (or singleindex, if specified)")
        System.exit(0)
      }

      @tailrec
      def mapFromArgsIter(argList: Array[String], argMap: Map[String, String]): Map[String, String] = argList match {
        case Array() => argMap
        case Array(h: String, _*) =>
          val firstColon =  h.indexOf(":")
          val key = h.substring(0, firstColon)
          val value = h.substring(firstColon+1, h.length)

          if(!defaultsMap.contains(key)) throw new Exception(s"Unrecognized argument: $key")

          mapFromArgsIter(argList.tail, argMap + (key -> value))
      }

      val valueMap = mapFromArgsIter(args, defaultsMap)

      ArgMap(
        singleindex = valueMap("singleindex"),
        esurl = valueMap("esurl"),
        languages = valueMap("languages"),
        sep = valueMap("sep")
      )
    }

    val startTime = System.currentTimeMillis()

    import java.nio.file.Path
    import java.nio.file.Paths
    val currentRelativePath = Paths.get("tessdata")
    val s = currentRelativePath.toAbsolutePath.toString
    System.out.println("Current relative path is: " + s)

    import sys.process._
    "ls" !

    index()
    //Await.result(index(), Duration.Inf)
    //new SQLConn(argMap("psqlhost"), argMap("psqluser"), argMap("psqlpass")).printPDFAndParticipants()

    println("took " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
    System.exit(0)
  }

  def index() = {

    val mapping = Future{
      val bufferedSource = io.Source.fromFile("input/mapping.csv")

      val lines: Iterator[String] = bufferedSource.getLines()

      lines.next()  //skip first line

      val map = lines.foldLeft(Map[String, List[String]]()) { (acc, line) =>  //map(pt, List of pdf)
        val idPDF = line.split(argMap.sep)

        if(acc.contains(idPDF(0))) acc + (idPDF(0) -> (idPDF(1) +: acc(idPDF(0))))
        else acc + (idPDF(0) -> List(idPDF(1)))
      }

      bufferedSource.close

      map
    }

    val clinicalData = Future{  //map(pt, pt data)
      val bufferedSource = io.Source.fromFile("input/clinicalData.csv")

      val lines: Iterator[String] = bufferedSource.getLines()

      val fields = lines.next().split(argMap.sep)

      val map = lines.foldLeft(Map[String, JsObject]()) { (acc, line) =>
        val asJson = JsObject(fields.zip(line.split(argMap.sep).map(Json.toJson(_))))
        acc + (asJson("participant").as[String] -> asJson)
      }

      bufferedSource.close

      map
    }

    val texts = { //map(pdf, pdf text)
      val OCRParser = new OCRParser(argMap.languages)

      val files = Walker.walk("input")

      Future.sequence(
        files.map{ file =>
          Future{
            OCRParser.parsePDF(file)
          }
        }
      ).map(files.map(_.getName()).zip(_).toMap)
    }

    val temperino = clinicalData.flatMap{ pts => //map(pdf, pt)
      mapping.flatMap{ ptPdfs => //map(pt, pt data)
        texts.map{ pdfs => //map(pdf, pdf text)

          val ESIndexer = new ESIndexer(argMap.esurl)

          pts.foldLeft(List[JsObject]()) { (acc, ptIdAndData) =>
            val (ptID, ptData) = ptIdAndData

            val listOfPdfs: List[String] = ptPdfs(ptID)

            val thePDFs = listOfPdfs.foldLeft(JsArray()) { (array: JsArray, pdfId) =>
              val x = Seq(
                ("pdf_key", JsString(pdfId)),
                ("pdf_text", JsString(pdfs(pdfId)))
              )

              JsObject(x) +: array

            }

            ESIndexer.index((ptData + ("pdfs" -> thePDFs)).toString())

            (ptData + ("pdfs" -> thePDFs)) +: acc
          }
        }
      }
    }

    val b = Await.result(temperino, Duration.Inf)

    b.foreach(println)

    val i = 0
  }
}