package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, Materializer}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Sample output
 *
 * warmup identity last value = 1000, time = 30ms
 * sync spin last value = 1000, time = 20002ms
 * parallel spin last value = 1000, time = 5128ms
 * sync identity last value = 1000, time = 2ms
 * parallel identity last value = 1000, time = 73ms
 */
object StreamsConcurrency extends App {
  implicit val system = ActorSystem("StreamsConcurrency")
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val source: Source[Long, NotUsed] = Source(1L to 1000L)

  // Simulate a uniform CPU-bound workload
  // source: https://blog.colinbreck.com/partitioning-akka-streams-to-maximize-throughput/
  def spin(value: Long): Long = {
    val start = System.currentTimeMillis()
    while (System.currentTimeMillis() - start < 20) {}
    value
  }

  def run(name: String, op: Flow[Long, Long, NotUsed]) = {
    val start = System.currentTimeMillis()
    val done = source
      .via(op)
      .runWith(Sink.last)
    done.foreach(v => println(s"$name last value = $v, time = ${System.currentTimeMillis() - start}ms"))
    Await.result(done, 100000.millisecond)
  }

  def sequential(fn: Long => Long) = Flow[Long].map(fn)

  def parallel(parallelism: Int, fn: Long => Long): Flow[Long, Long, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val balancer = builder.add(Balance[Long](parallelism))
    val merger = builder.add(Merge[Long](parallelism))

    for (_ <- 0 until parallelism) {
      balancer ~> sequential(fn).async ~> merger
    }

    FlowShape(balancer.in, merger.out)
  })

  run("warmup identity", sequential(identity))

  run("sync spin", sequential(spin))
  run("parallel spin", parallel(4, spin))

  run("sync identity", sequential(identity))
  run("parallel identity", parallel(4, identity))
}