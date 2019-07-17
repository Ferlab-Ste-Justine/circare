import java.io.{File, FileInputStream}
import java.util.regex.Pattern

import opennlp.tools.lemmatizer.DictionaryLemmatizer
import opennlp.tools.postag.{POSModel, POSTaggerME}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}

class NLPParser {

  //https://www.tutorialspoint.com/opennlp/opennlp_finding_parts_of_speech.htm
  //TODO how can we use mutiple languages in the same model??

  //https://sites.google.com/site/nicolashernandez/resources/opennlp

  /*
  Notes on thread-safety: openNLP is NOT thread-safe! The only thing we can share is the models.

  https://stackoverflow.com/questions/4989381/nullpointer-exception-with-opennlp-in-namefinderme-class
  https://stackoverflow.com/questions/22785942/nullpointerexception-while-using-postaggerme-of-opennlp-in-android

  The rest has to be instanciated on a thread-by thread basis
   */
  val enPosModel = new POSModel(new FileInputStream("./nlp/en-pos-maxent.bin"))
  val enTokenModel = new TokenizerModel(new FileInputStream("./nlp/en-token.bin"))

  //https://raw.githubusercontent.com/richardwilly98/elasticsearch-opennlp-auto-tagging/master/src/main/resources/models/en-lemmatizer.dict
  val dictFile: File = new File("nlp/en-lemmatizer.dict")

  /**
    * Gets the lemmas of the text (useful for keywordisation).
    *
    * Lemmas are the generic forms of words
    *
    * https://nlp.stanford.edu/IR-book/html/htmledition/stemming-and-lemmatization-1.html
    *
    * @param text the input text
    * @return the lemmas of it's NN/NNS
    */
  def getLemmas(text: String): Seq[String] = {

    val tokens = new TokenizerME(enTokenModel).tokenize(text)
    val tags = new POSTaggerME(enPosModel).tag(tokens)

    val filteredTokenTags = getFilteredTT(tokens, tags)

    val tempLemmas = new DictionaryLemmatizer(dictFile).lemmatize(filteredTokenTags._1, filteredTokenTags._2)

    /*
    When the lemmatizer can't find a word, it outputs "O". We want to replace the O's with their original words
    (found in nounTag).

    We could technically zip tempLemmas with nountag and then use foldLeft with a case match; but that would be
    pretty inefficient. Instead, we're going imperative-style and using the index of the O's to grab the word.
     */

    tempLemmas.indices.foldLeft(Set[String]()) { (acc: Set[String], i: Int) =>
      val lemma: String = tempLemmas(i)

      if (lemma.equals("")) acc
      else if (lemma.equals("O")) acc + filteredTokenTags._1(i)
      else acc + lemma
    }.toSeq
  }

  private def getFilteredTT(tokens: Array[String], tags: Array[String]) = {

    /**
      * Is the tag an important tag?
      *
      * https://medium.com/@gianpaul.r/tokenization-and-parts-of-speech-pos-tagging-in-pythons-nltk-library-2d30f70af13b
      *
      * @param tag the tag
      * @return whether or not the tag is important
      */
    def isKeytag(tag: String): Boolean = List("NNP", "NNPS", "NN", "NNS", "FW").contains(tag)

    /**
      * Can this string be considered a number? / Is this a numeric string?
      *
      * @param str the string
      * @return wether or not it's a number
      */
    def isNumeric(str: String): Boolean = {
      try {
        Integer.valueOf(str)
        true
      } catch {
        case _: Exception => false
      }
    }

    /**
      * In our dataset the only interesting words of length two and lower are bone identifiers.
      *
      * @param str the word
      * @return wether or not it's a bone ID
      */
    def isBoneID(str: String): Boolean = Pattern.matches("[a-z][0-9]+", str)

    tokens.indices.foldLeft((Array[String](), Array[String]())) { (acc: (Array[String], Array[String]), i: Int) =>
      //lazy val: if isKeytag fails we don't need to calculate token
      lazy val token: String = tokens(i).replaceAll("[,\\/#!$%\\^&\\*;|:{}=\\-_`~()\\[\\]<>\"”(\\.$)]", "").toLowerCase
      val tag = tags(i)

      if (isKeytag(tag) && (token.length >= 3 || isBoneID(token)) && !isNumeric(token)) (acc._1 :+ token, acc._2 :+ tag)
      else acc
    }
  }
}
