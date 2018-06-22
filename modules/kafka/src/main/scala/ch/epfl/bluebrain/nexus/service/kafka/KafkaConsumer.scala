package ch.epfl.bluebrain.nexus.service.kafka

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.kafka.ConsumerSettings
import akka.pattern.BackoffSupervisor.{CurrentChild, GetCurrentChild}
import akka.pattern.{Backoff, BackoffSupervisor, ask}
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexFailuresLog
import io.circe.Decoder

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object KafkaConsumer {

  /**
    * Starts a Kafka consumer that reads messages from a particular ''topic'', decodes them into
    * events of type ''Event'' and indexes them using the provided function.  The consumer stream is
    * wrapped in its own actor whose life-cycle is managed by a [[akka.pattern.BackoffSupervisor]] instance.
    *
    * @param settings    an instance of [[akka.kafka.ConsumerSettings]]
    * @param index       the indexing function that is applied to received events
    * @param topic       the Kafka topic to read messages from
    * @param name        a valid and __unique__ prefix for the supervisor and child actor names
    * @param decoder     a Circe decoder to deserialize received messages
    * @param committable indicates whether the consumer should use a committable source
    * @param failuresLog an optional [[ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexFailuresLog]]
    * @param as          an implicitly available actor system
    * @tparam Event      the event generic type
    * @return the supervisor actor handle
    */
  def start[Event](
      settings: ConsumerSettings[String, String],
      index: Event => Future[Unit],
      topic: String,
      name: String,
      committable: Boolean = true,
      failuresLog: Option[IndexFailuresLog] = None)(implicit as: ActorSystem, decoder: Decoder[Event]): ActorRef = {
    val childProps =
      Props(classOf[KafkaSupervisedConsumer[Event]], settings, index, topic, decoder, committable, failuresLog)
    val supervisor = as.actorOf(
      BackoffSupervisor.props(
        Backoff.onStop(
          childProps,
          childName = s"$name-child",
          minBackoff = 3.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.2
        )),
      name = s"$name-supervisor"
    )

    supervisor
  }

  /**
    * Kills the managed actor that wraps the Kafka consumer, which should be re-spawned by its supervisor.
    *
    * @param supervisor the supervisor actor handle
    */
  private[kafka] def stop(supervisor: ActorRef)(implicit as: ActorSystem): Unit = {
    implicit val ec: ExecutionContext = as.dispatcher
    implicit val to: Timeout          = 30.seconds
    (supervisor ? GetCurrentChild).mapTo[CurrentChild].foreach(_.ref.foreach(as.stop))
  }

}
