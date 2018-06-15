package ch.epfl.bluebrain.nexus.service.queue

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.kafka.ConsumerSettings
import akka.pattern.BackoffSupervisor.{CurrentChild, GetCurrentChild}
import akka.pattern.{Backoff, BackoffSupervisor, ask}
import akka.util.Timeout
import io.circe.Decoder

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object KafkaConsumer {

  /**
    * Starts a Kafka consumer that reads messages from a particular ''topic'', decodes them into
    * events of type ''Event'' and indexes them using the provided function.  The consumer stream is
    * wrapped in its own actor whose life-cycle is managed by a [[akka.pattern.BackoffSupervisor]] instance.
    *
    * @param settings an instance of [[akka.kafka.ConsumerSettings]]
    * @param index    the indexing function that is applied to received events
    * @param topic    the Kafka topic to read messages from
    * @param decoder  a Circe decoder to deserialize received messages
    * @param as       an implicitly available actor system
    * @tparam Event   the event generic type
    * @return the supervisor actor handle
    * @note  Calling this method multiple times within the same actor system context will result in
    *        an [[akka.actor.InvalidActorNameException]] being thrown. You must stop any previously existing
    *        supervisor first.
    */
  def start[Event](settings: ConsumerSettings[String, String], index: Event => Future[Unit], topic: String)(
      implicit as: ActorSystem,
      decoder: Decoder[Event]): ActorRef = {
    val childProps = Props(classOf[KafkaSupervisedConsumer[Event]], settings, index, topic, decoder)
    val supervisor = as.actorOf(
      BackoffSupervisor.props(
        Backoff.onStop(
          childProps,
          childName = "kafka-stream-supervised-actor",
          minBackoff = 3.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.2
        )),
      name = "kafka-stream-supervisor"
    )

    supervisor
  }

  /**
    * Kills the managed actor that wraps the Kafka consumer, which should be re-spawned by its supervisor.
    *
    * @param supervisor the supervisor actor handle
    */
  private[queue] def stop(supervisor: ActorRef)(implicit as: ActorSystem): Unit = {
    implicit val ec: ExecutionContext = as.dispatcher
    implicit val to: Timeout          = 30.seconds
    (supervisor ? GetCurrentChild).mapTo[CurrentChild].foreach(_.ref.foreach(as.stop))
  }

}
