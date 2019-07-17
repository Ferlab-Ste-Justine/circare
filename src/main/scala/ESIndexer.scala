import org.apache.http.HttpHost
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.{ActionListener, ActionResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.Strings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.common.xcontent.XContentType

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

class ESIndexer(url: String = "http://localhost:9200") {
  //https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-index.html
  //https://www.elastic.co/guide/en/elasticsearch/reference/7.0/docs-index_.html
  //https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
  //https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-document-bulk.html

  val esClient = new RestHighLevelClient(RestClient.builder(HttpHost.create(url)))

  //https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-create-index.html
  //https://discuss.elastic.co/t/elasticsearch-total-term-frequency-and-doc-count-from-given-set-of-documents/115223
  //https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-termvectors.html

  initIndexes()

  def initIndexes(): Unit = {

    val exists = new GetIndexRequest()
    exists.indices("qsearch")

    if (esClient.indices().exists(exists, RequestOptions.DEFAULT)) return //if index exists, stop

    val jsonAdminFileLemma = jsonBuilder

    jsonAdminFileLemma.startObject()
    jsonAdminFileLemma.startObject("_doc")

      jsonAdminFileLemma.startObject("properties")

        jsonAdminFileLemma.startObject("pdf_text")
        jsonAdminFileLemma.field("type", "text")
        jsonAdminFileLemma.field("analyzer", "english") //use the custom analyser we're creating in jsonSettings
        jsonAdminFileLemma.endObject()

        jsonAdminFileLemma.startObject("pdf_words")
        jsonAdminFileLemma.field("type", "keyword")
        jsonAdminFileLemma.endObject()

        jsonAdminFileLemma.startObject("pdf_type")
        jsonAdminFileLemma.field("type", "keyword")
        jsonAdminFileLemma.endObject()

        jsonAdminFileLemma.startObject("pdf_id")
        jsonAdminFileLemma.field("type", "keyword")
        jsonAdminFileLemma.endObject()

        jsonAdminFileLemma.startObject("pdf_key")
        jsonAdminFileLemma.field("type", "keyword")
        jsonAdminFileLemma.endObject()

      jsonAdminFileLemma.endObject()

    jsonAdminFileLemma.endObject()
    jsonAdminFileLemma.endObject()

    val request = new CreateIndexRequest("qsearch")
    request.mapping("_doc",
      Strings.toString(jsonAdminFileLemma),
      XContentType.JSON)

    request.settings(Settings.builder().put("index.number_of_shards", 1))

    /*
    Try to create the index. If it already exists, don't do anything
     */
    try {
      esClient.indices().create(request, RequestOptions.DEFAULT)
    } catch {
      case e: Exception => //e.printStackTrace()
    }
  }

  /**
    * Makes an ES IndexRequest from an IndexingRequest
    *
    * @param req
    * @return
    */
  private def makeIndexRequest(req: String) = {
    val request = new IndexRequest("qsearch")
    request.`type`("_doc")
    request.source(req, XContentType.JSON)

    request
  }

  def bulkIndex(reqs: Seq[String]): Unit = {

    val bulked = new BulkRequest()

    reqs.foreach(req => bulked.add(makeIndexRequest(req)))

    esClient.bulk(bulked, RequestOptions.DEFAULT)
  }
}
