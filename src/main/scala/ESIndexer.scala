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

import Main.argMap

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
    exists.indices(argMap.esindex)

    if (esClient.indices().exists(exists, RequestOptions.DEFAULT)) return //if index exists, stop

    val props = jsonBuilder

    props.startObject()
    props.startObject("_doc")

      props.startObject("properties")

        props.startObject("pdfs")

          props.startObject("properties")
            props.startObject("pdf_text")
            props.field("type", "text")
              props.startObject("fields")

              props.startObject("en")
                props.field("type", "text")
                props.field("analyzer", "english")
              props.endObject()

              props.startObject("fr")
                props.field("type", "text")
                props.field("analyzer", "french")
              props.endObject()

              props.endObject()
            props.endObject()
          props.endObject()

        props.endObject()

      props.endObject()

    props.endObject()
    props.endObject()

    val request = new CreateIndexRequest(argMap.esindex)
    request.mapping("_doc",
      Strings.toString(props),
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
    val request = new IndexRequest(argMap.esindex)
    request.`type`("_doc")
    request.source(req, XContentType.JSON)

    request
  }

  def index(req: String): Unit = esClient.index(makeIndexRequest(req), RequestOptions.DEFAULT)

  def bulkIndex(reqs: Seq[String]): Unit = {

    val bulked = new BulkRequest()

    reqs.foreach(req => bulked.add(makeIndexRequest(req)))

    esClient.bulk(bulked, RequestOptions.DEFAULT)
  }
}
