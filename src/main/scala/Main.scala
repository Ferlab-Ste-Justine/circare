import java.io.{ByteArrayOutputStream, File, PrintStream}

import play.api.libs.json.{JsArray, JsObject, JsString, Json}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions

object Main {

  case class ArgMap(esurl: String = "http://localhost:9200", esindex: String = "qsearch", languages: String = "eng",
                    sep: String = ",", printascsv: String = "false", verbose: String = "true")

  var argMap: ArgMap = _

  def init(args: Array[String]): Unit = {
    //GRABBING ARGS FROM CLI INTO CASE CLASS
    import CaseClassUtils._

    val defaultsMap = ArgMap().toMap

    if (args.contains("--help") || args.contains("--h") || args.contains("-help") || args.contains("-h")) {
      def printtab(string: String): Unit = print(s"\t$string")

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
      "org.apache.fontbox.ttf.PostScriptTable",
      "org.elasticsearch.client.RestClient"
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

    index()

    System.exit(0)
  }

  def index(): Unit = {

    def println(string: Any): Unit = if(argMap.verbose.equals("true")) scala.Predef.println(string)
    def print(string: Any): Unit = if(argMap.verbose.equals("true")) scala.Predef.print(string)
    def printtab(string: Any): Unit = print(s"\t$string")
    def printtabln(string: Any): Unit = printtab(s"$string\n")

    println("Reading mapping...")

    val mapping = CSVUtils.readCSVSkip("input/mapping.csv", argMap.sep).foldLeft(Map[String, List[String]]()) { (acc, line) =>  //map(pt, List of pdf)
      printtabln(line.mkString(","))

      if(acc.contains(line(0))) acc + (line(0) -> (line(1) +: acc(line(0))))
      else acc + (line(0) -> List(line(1)))
    }

    println("Reading clinical data...")

    val clinicalData = {  //map(pt, pt data)
      val csv = CSVUtils.readCSV("input/clinicalData.csv", argMap.sep)

      val fields = csv.next()

      csv.foldLeft(Map[String, JsObject]()) { (acc, line) =>
        printtabln(line.mkString(","))

        val asJson = JsObject(fields.zip(line.map(Json.toJson(_))))
        acc + (asJson("participant_id").as[String] -> asJson)
      }
    }

    println("OCRing PDFs. This is an async operation.")
    println("Final output will be printed in the requested format as we go")

    var nbOfPDFsDone: Int = 0
    val nbOfPDFs = mapping.values.toList.length

    val texts = { //map(pdf, pdf text)
      val OCRParser = new OCRParser(argMap.languages)
      val ESIndexer = new ESIndexer(argMap.esurl)
      lazy val fields = clinicalData(clinicalData.keys.head).keys.toList :+ "pdfs" //the names of the columns needed if we print as csv
      if(argMap.printascsv.equals("true")) println(fields.mkString(argMap.sep))

      Future.sequence(
        clinicalData.map{ case (p, pdata) =>
          Future{
            val ocred: JsArray = mapping(p).foldLeft(JsArray()){ (acc, f) =>
              val file = new File("input/"+f)

              val text = OCRParser.parsePDF(file)

              JsObject(
                Seq(
                  ("pdf_key", JsString(f)),
                  ("pdf_text", JsString(text))
                )
              ) +: acc
            }

            val asJson = pdata + ("pdfs" -> ocred)
            val asString = asJson.toString()
            ESIndexer.index(asString)

            if(argMap.printascsv.equals("false")) scala.Predef.println(asString)
            else {
              scala.Predef.println(
                fields.map(asJson(_).toString().replaceAll("^\"|\"$", "")).mkString(argMap.sep)
              )
            }
          }
        }
      )
    }

    Await.result(texts, Duration.Inf)

  }
}