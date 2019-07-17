import java.io.File

import org.scalatest.{FlatSpec, Matchers}

class OCRParserTest extends FlatSpec with Matchers {
  val ocrParser = new OCRParser


  "ocr" should "return \"Brain cancer is bad. This sucks: it is not fun, man...\"\n with input brainCancer.pdf" in {
    val result = ocrParser.parsePDF(new File("./testInput/brainCancer.pdf"))
    result shouldBe "\"Brain cancer is bad. This sucks: it is not fun, man...\"\n"
  }
}
