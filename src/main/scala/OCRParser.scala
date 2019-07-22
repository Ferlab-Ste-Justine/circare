import java.io.{ByteArrayInputStream, File, FileInputStream, InputStream}

import javax.imageio.ImageIO
import javax.imageio.spi.IIORegistry
import net.sourceforge.tess4j.Tesseract
import org.apache.pdfbox.jbig2.JBIG2ImageReaderSpi
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.rendering.{ImageType, PDFRenderer}
import org.apache.pdfbox.text.PDFTextStripper

class OCRParser(languages: String = "eng") {

  /*
      Bug on tesseract 4.0.0: crash when calling procedure is not tesseract itself. To fix, we're changing the locale
      to C using java's Native library (see interface CLibrary.java)

      https://github.com/nguyenq/tess4j/issues/106
  */
  CLibrary.INSTANCE.setlocale(CLibrary.LC_ALL, "C")
  CLibrary.INSTANCE.setlocale(CLibrary.LC_NUMERIC, "C")
  CLibrary.INSTANCE.setlocale(CLibrary.LC_CTYPE, "C")

  /**
    * Syntactic sugar for parsePDF(pdf: Stream)
    *
    * @param pdf the java.io.File to input to parsePDF
    * @return the text of the pdf
    */
  def parsePDF(pdf: File): String = parsePDF(new FileInputStream(pdf))

  /**
    * Parses the requested pdf stream into a string.
    *
    * Uses direct extraction if possible and OCR otherwise
    *
    * @param pdf The pdf (as a stream)
    * @return The string (text) of the pdf
    */
  @throws[java.io.IOException]
  def parsePDF(pdf: InputStream): String = {

    val document =
      try {
        PDDocument.load(pdf)

      } catch {
        case _: Exception =>
          //https://stackoverflow.com/questions/34805134/pdfbox-1-8-10-error-in-generating-pddocument-from-load-method

          /*
          We're going to /maybe/ fix the file by prepending as PDF marker to it. If it doesn't work, the file
          is beyond repair
           */
          val fixed: Array[Byte] = Array[Byte](25, 50, 44, 46) ++ pdf.readAllBytes()

          PDDocument.load(new ByteArrayInputStream(fixed))
      }

    /*
    Try-catch-finally in scala returns whichever value of try catch works

    https://stackoverflow.com/questions/18685573/try-catch-finally-return-value/18685727
     */

    val directText = new PDFTextStripper().getText(document)
    val text = if (directText.length < 300) ocrExtract(document) else directText

    document.close() //need to close the document before exiting this function

    text
  }

  /**
    * Extracts text from the document using OCR. This method always succeeds but is more imprecise than directExtract
    *
    * @param document the docuement to do the OCR on
    * @return the text of the document
    */
  private def ocrExtract(document: PDDocument): String = {
    /*
    https://sourceforge.net/p/tess4j/discussion/1202293/thread/4562eccb/

    Each thread needs its tesseract
     */

    val tesseract = {
      val tesseract: Tesseract = new Tesseract() //the Tesseract used to do the OCR.
      tesseract.setDatapath("./tessdata") //we're using the local tessdata folder (fails when using global...)
      tesseract.setLanguage(languages) //by default we're reading English (TODO add more tessdata)

      tesseract
    }

    ImageIO.scanForPlugins()
    IIORegistry.getDefaultInstance.registerServiceProvider(new JBIG2ImageReaderSpi)
    val renderer = new PDFRenderer(document)

    /*
    Ideally, the renderer would offer a entireDocumentAsImage method or a getAllPagesAsImages; or it could take a
    single page and render it.

    Unfornuately, it doesn't, so we either have to map on the List of the indexes of the document's pages, or we
    have to use parseRec to recursively OCR every page. Both methods use the indexes of the pages, but the map
    is more readable.

    The real best way would simply be an Imperative-style for loop, though, avoiding having to create intermediary
    data structures...

    TesseractOCR throws errors/warnings when lines are smaller than magic number 3. So, we're scaling every document
    to 3 times its size. (This is my solution; there might be a better one but I haven't found one)

    https://stackoverflow.com/questions/52423848/tesseract-in-r-does-not-recognize-smaller-fonts-in-the-same-document
    https://groups.google.com/forum/#!msg/tesseract-ocr/VEaEftAfD3Y/thx2F51cCQAJ

    Also, we only need a grayscale image to do OCR, so we're saving on memory with ImageType.GRAY
     */

    var asText = ""
    for (i <- 0 until document.getNumberOfPages) asText += tesseract.doOCR(renderer.renderImage(i, 3.1f, ImageType.GRAY))

    asText
  }


}