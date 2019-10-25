package example

import java.time.{LocalDateTime, LocalTime}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{ActorMaterializer, Attributes, DelayOverflowStrategy}

import scala.concurrent.duration._

object TimeWindowComplexEventAggregator extends App {

  implicit val system = ActorSystem("TimeWindowComplexEventAggregator")
  implicit val materializer = ActorMaterializer()

  sealed trait Event
  case object A extends Event
  case object B extends Event
  case object C extends Event
  val allEvents: Set[Event] = Set(A,B,C)


  // any objects that flow into the statefulWindow must be of the same type so we constrain pattern matching correctly
  sealed trait WindowMessage

  final case class BusinessMessage(key: Int, event: Event, timestamp: Long) extends WindowMessage
  case object TickEvent extends WindowMessage

  final case class Alert(key: Int, time: LocalTime)

  def messageGeneratorSource(): Source[BusinessMessage, NotUsed] = Source(1 to 100)
    // slow down elements so they can be grouped by the window length
    .delay(1 second, DelayOverflowStrategy.backpressure)
    .map { i =>
      val key = i % 3
      // randomly pick an event
      val event = scala.util.Random.nextInt(3) match {
        case 0 => A
        case 1 => B
        case 2 => C
      }
      BusinessMessage(key, event, System.currentTimeMillis())
    }

  // returns true when this keys "Set(all events) - Set(events) > 0"
  def doesNotContainAllEvents(events: Set[Event]): Boolean = allEvents.diff(events).nonEmpty

  // encapsulates the sliding window logic and the state
  def statefulWindow(windowLength: FiniteDuration): Flow[WindowMessage, Alert, NotUsed] = Flow[WindowMessage]
    // eagerComplete will shutdown the stream with any shutdown signal (i.e. so this shuts down when all source elements have been emitted
    .merge(Source.tick[WindowMessage](windowLength, windowLength, TickEvent), eagerComplete = true)
    .statefulMapConcat { () =>
      var acc = Map[Int, Set[Event]]()

      {
        case BusinessMessage(key, event, _) =>
          val keyEvents = acc.getOrElse(key, Set[Event]())
          // add this messages event to the accumulator of key events
          acc = acc + (key -> (keyEvents + event))
          // we return nothing because we only emit downstream when a `TickEvent` occurs and our acc state is met
          Nil
        case TickEvent =>
          val time: LocalTime = LocalDateTime.now().toLocalTime
          println(s"TickEvent. Time: '$time'. Acc: $acc")
          val alerts: List[Alert] = for {
            (key, events: Set[Event]) <- acc.toList if doesNotContainAllEvents(events)
          } yield Alert(key, time)

          // reset accumulator
          acc = Map.empty

          // emit alerts, if any were found
          alerts
      }
    }

  val windowLength = 10 seconds
  
  messageGeneratorSource()
    .via(statefulWindow(windowLength = 10 seconds))
    // artificially shrink buffer to 1 for example (default buffer is 16)
    .addAttributes(Attributes.inputBuffer(1,1))
    .runForeach { case Alert(key, time) =>
      println(s"Alert, key '$key' at '$time', did not contain all events within '$windowLength'")
    }
}
