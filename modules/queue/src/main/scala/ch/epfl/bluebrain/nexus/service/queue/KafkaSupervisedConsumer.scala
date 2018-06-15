package ch.epfl.bluebrain.nexus.service.queue

import java.util.concurrent.TimeUnit.SECONDS

import akka.actor.Actor.emptyBehavior
import akka.actor._
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch, GroupTopicPartition}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.persistence.query.Sequence
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexFailuresLog
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryOps._
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.Backoff
import io.circe._
import io.circe.parser._
import journal.Logger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Do not instantiate this class directly, see [[KafkaConsumer.start]].
  */
class KafkaSupervisedConsumer[Event](settings: ConsumerSettings[String, String],
                                     index: Event => Future[Unit],
                                     topic: String,
                                     decoder: Decoder[Event])
    extends Actor {

  private implicit val as: ActorSystem       = context.system
  private implicit val ec: ExecutionContext  = as.dispatcher
  private implicit val mt: ActorMaterializer = ActorMaterializer()

  private val log     = Logger[this.type]
  private val config  = context.system.settings.config.getConfig("indexing.retry")
  private val retries = config.getInt("max-count")
  private val strategy =
    Backoff(Duration(config.getDuration("max-duration", SECONDS), SECONDS), config.getDouble("random-factor"))
  private val failures = IndexFailuresLog(s"kafka-consumer-$topic")

  override val receive: Receive = emptyBehavior

  override def preStart(): Unit = {
    val done = Consumer
      .committableSource(settings, Subscriptions.topics(topic))
      .mapAsync(1) { msg =>
        process(msg)
          .recover { case e => log.error(s"Unexpected failure while processing message: $msg", e) }
          .map(_ => msg.committableOffset)
      }
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
        batch.updated(elem)
      }
      .mapAsync(1)(_.commitScaladsl())
      .runWith(Sink.ignore)

    done.onComplete {
      case Failure(e) =>
        log.error("Stream failed with an error, stopping the actor and restarting.", e)
        kill()
      // $COVERAGE-OFF$
      case Success(_) =>
        log.warn("Kafka consumer stream ended gracefully, however this should not happen; restarting.")
        kill()
      // $COVERAGE-ON$
    }
  }

  private def kill(): Unit = self ! PoisonPill

  private def process(msg: CommittableMessage[String, String]): Future[Unit] = {
    val value = msg.record.value
    parse(value).flatMap(decoder.decodeJson) match {
      case Right(event) =>
        log.debug(s"Received message: $value")
        (() => index(event))
          .retry(retries)(strategy)
          .recoverWith {
            // $COVERAGE-OFF$
            case e =>
              log.error(s"Failed to index event: $event; skipping.", e)
              storeFailure(msg)
          }
      case Left(e) =>
        log.error(s"Failed to decode message: $value; skipping.", e)
        storeFailure(msg)
      // $COVERAGE-ON$
    }
  }

  private def storeFailure(msg: CommittableMessage[String, String]): Future[Unit] = {
    // $COVERAGE-OFF$
    val offset                                       = Sequence(msg.committableOffset.partitionOffset.offset)
    val GroupTopicPartition(group, topic, partition) = msg.committableOffset.partitionOffset.key
    failures.storeEvent(s"$group-$topic-$partition", offset, msg.record.value)
    // $COVERAGE-ON$
  }

}
