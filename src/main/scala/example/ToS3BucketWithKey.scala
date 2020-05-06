package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import example.GroupedBatchToS3.stream

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object ToS3BucketWithKey extends App {
  implicit val system = ActorSystem("ToS3BucketWithKey")
  implicit val materializer = ActorMaterializer()

  type JsValue
  type Company

  val stringToJSON: Flow[String, JsValue, NotUsed] = ???
  val jsonToCompany: Flow[JsValue, Company, NotUsed] = ???
  val makeCompanyToByteString: Flow[Company, (String, ByteString), NotUsed] = Flow[Company].map { company: Company =>
    val companyByteString: ByteString = ??? // convert the company to a bytestring somehow
    (company.hashCode().toString, companyByteString)
  }

  val sampleSource: Source[String, NotUsed] = Source(List("4-5-6", "7-8-12", "14-9-10"))

  sampleSource
    .via(stringToJSON)
    .via(jsonToCompany)
    .via(makeCompanyToByteString)
    // the parallelism will limit how many concurrent uploads to S3 to do at a time
    .mapAsync(parallelism = 47) {
      case (companyHash, companyByteString) =>
        val result: Future[MultipartUploadResult] = Source.single(companyByteString)
            .toMat(S3.multipartUpload("sample_topic", companyHash))(Keep.right)
            .run()

        result
    }
    .map { s3uploadResult: MultipartUploadResult =>
      // do something with the response of the result future
    }
    .runWith(Sink.ignore)

  Await.result(stream, 1.minute)
}
