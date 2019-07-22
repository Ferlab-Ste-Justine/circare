import java.io.{ByteArrayOutputStream, PrintStream}

import play.api.libs.json.{JsArray, JsObject, JsString, Json}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Main {

  case class ArgMap(esurl: String = "http://localhost:9200", esindex: String = "qsearch", languages: String = "eng", sep: String = ",", printascsv: String = "false")

  var argMap: ArgMap = _

  def init(args: Array[String]): Unit = {
    //GRABBING ARGS FROM CLI INTO CASE CLASS
    import CaseClassUtils._

    val defaultsMap = ArgMap().toMap

    println(s"You entered these arguments: ${args.mkString(" ")}")

    if (args.contains("--help") || args.contains("--h") || args.contains("-help") || args.contains("-h")) {
      println("Specify your arguments using this notation => arg:value.")
      println("Values you can specify and their defaults:")
      defaultsMap.foreach{ case (k, v) => println(s"\t$k:$v") }
      println("Special notes about some of these arguments:")
      printtab("languages: must be of form lang1+lang2+lang3. Currently, only fra, eng, and osd are supported.\nThe more languages, the slower the OCR is.")
      printtab("sep: sep is used for ALL CSV files")
      printtab("verbose: true of false")
      printtab("printascsv: if true, will print the indexed json as a CSV using the provided sep. Do note that json arrays will stay json arrays.\nIf false, prints as json")

      System.exit(0)
    }

    @tailrec
    def mapFromArgsIter(argList: Array[String], argMap: Map[String, String]): Map[String, String] = argList match {
      case Array() => argMap
      case Array(h: String, _*) =>
        val firstColon =  h.indexOf(":")

        if(firstColon == -1) throw new Exception(s"Unrecognized argument: $h")

        val key = h.substring(0, firstColon)
        val value = h.substring(firstColon+1, h.length)

        if(!defaultsMap.contains(key)) throw new Exception(s"Unrecognized argument key: $key")

        mapFromArgsIter(argList.tail, argMap + (key -> value))
    }

    argMap = ArgMap().fromMap(mapFromArgsIter(args, defaultsMap))

    //DISABLING MOST LOGGING
    import ch.qos.logback.classic.{Level, Logger}
    import org.slf4j.LoggerFactory
    //https://github.com/jfrog/artifactory-client-java/issues/77
    val loggers = Seq(
      "org.apache.http",
      "org.apache.pdfbox.io.ScratchFileBuffer",
      "org.apache.pdfbox.pdfparser.PDFObjectStreamParser",
      "org.apache.pdfbox.pdmodel.font.PDType1Font",
      "org.apache.fontbox.ttf.GlyphSubstitutionTable",
      "org.apache.fontbox.util.autodetect.FontFileFinder",
      "org.apache.pdfbox.pdmodel.font.PDType1Font",
      "org.apache.pdfbox.pdmodel.graphics.color.PDDeviceRGB",
      "org.apache.fontbox.ttf.PostScriptTable"
    )

    loggers.foreach { name =>
      val logger = LoggerFactory.getLogger(name).asInstanceOf[Logger]
      logger.setLevel(Level.INFO)
      logger.setAdditive(false)
    }

    //DONE!
  }

  def main(args: Array[String]) {

    init(args)

    val startTime = System.currentTimeMillis()

    index()

    println("took " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
    System.exit(0)
  }

  def printtab(string: String): Unit = print(s"\t$string")

  def index(): Unit = {

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

    val f = clinicalData.flatMap{ pts => //map(pdf, pt)
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

    val done = Await.result(f, Duration.Inf)

    if(argMap.printascsv.equals("true")) {

      val fields = done.head.keys

      println(fields.mkString(argMap.sep))

      done.foreach{ json =>
        println(
          fields.map(f => json(f).toString().replaceAll("^\"|\"$", "")).mkString(argMap.sep)
        )
      }

    } else done.foreach(println)

  }
}