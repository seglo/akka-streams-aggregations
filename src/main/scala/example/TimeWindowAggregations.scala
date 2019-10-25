package example

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Attributes, DelayOverflowStrategy}

import scala.concurrent.duration._

object TimeWindowAggregations extends App {

  implicit val system = ActorSystem("TimeWindowAggregations")
  implicit val materializer = ActorMaterializer()

  sealed trait Event

  final case class IntegerEvent(i: Int) extends Event
  case object TickEvent extends Event

  val windowLength = 5 seconds

  Source(1 to 100)
    // slow down elements so they can be grouped by the window length
    .delay(1 second, DelayOverflowStrategy.backpressure)
    .map[Event](IntegerEvent)
    // eagerComplete will shutdown the stream with any shutdown signal (i.e. so this shuts down when all source elements have been emitted
    .merge(Source.tick[Event](windowLength, windowLength, TickEvent), eagerComplete = true)
    .statefulMapConcat { () =>
      var acc = List[IntegerEvent]()

      {
        case i: IntegerEvent =>
          acc = acc :+ i
          Nil
        case TickEvent =>
          val accToEmit = acc
          acc = Nil
          List(accToEmit)
      }
    }
    // artificially shrink buffer to 1 for example (default buffer is 16)
    .addAttributes(Attributes.inputBuffer(1,1))
    .runForeach { window =>
      println(s"Window [${window.map(_.i).mkString(",")}]")
    }
}
