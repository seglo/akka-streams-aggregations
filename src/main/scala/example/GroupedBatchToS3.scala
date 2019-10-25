package example

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.s3.S3Headers
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

// i put together some pseudo-code and it seems that you coudl get away with using groupedWithin, unless the weighted cost function in groupedWeightedWithin is useful to you
object GroupedBatchToS3 extends App {
  implicit val system = ActorSystem("GroupedBatchToS3")
  implicit val materializer = ActorMaterializer()

  val s3Headers = S3Headers()

  val stream: Future[Done] = Source(1 to 100)
    // group by a max of 10 messages or 30 seconds pass (with at least one msg), whichever comes first
    .groupedWithin(10, 30.seconds)
    // only write one batch at a time
    .mapAsync(parallelism = 1) { batch =>
      // encode beginning and end offset of batch into object key
      val objName: String = batch match {
        case head :: Nil => s"batch-$head"
        case head :: tail => s"batch-$head-to-${tail.last}"
        // `groupedWithin` asserts empty batches are not emitted
        case Nil => throw new Exception("Should never happen")
      }

      // encode data into bytes, separated by tabs
      val bytes = ByteString(batch.mkString("\t"))
      // create a source for Alpakka S3 to stream
      val dataSource: Source[ByteString, NotUsed] = Source.single(bytes)

      // run Alpakka S3 putObject stream within this stage
      S3
        .putObject(
          bucket = "bucket",
          key = objName,
          data = dataSource,
          contentLength = bytes.length,
          s3Headers = s3Headers)
        .runWith(Sink.head)
    }
    // do something with the S3 put response
    .map(identity)
    .runWith(Sink.ignore)

  Await.result(stream, 1.minute)
}