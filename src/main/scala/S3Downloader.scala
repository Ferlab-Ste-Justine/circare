import java.io.{File, FileInputStream, InputStream}

import com.amazonaws.SdkClientException
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{GetObjectRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import scala.annotation.tailrec


class S3Downloader {
  //https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/TransferManager.html#downloadDirectory-java.lang.String-java.lang.String-java.io.File-
  //https://stackoverflow.com/questions/49116960/download-all-the-files-from-a-s3-bucket-using-scala
  //https://docs.aws.amazon.com/AmazonS3/latest/dev/RetrievingObjectUsingJava.html

  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withRegion(Regions.US_EAST_1)
    .build()

  def download(bucketName: String, key: String,  retries: Int = 10): InputStream = {

    @tailrec
    def iter(tries: Int): InputStream = {
      try {
        val obj: S3Object = s3Client.getObject(new GetObjectRequest(bucketName, key))

        println("S3Object content type: " + obj.getObjectMetadata.getContentType)

        obj.getObjectContent
      } catch {
        case _: SdkClientException =>
          if(tries < retries) iter(tries+1)
          else new FileInputStream(new File("./defaultPDF/default.pdf"))
      }
    }

    iter(0)
  }
}
