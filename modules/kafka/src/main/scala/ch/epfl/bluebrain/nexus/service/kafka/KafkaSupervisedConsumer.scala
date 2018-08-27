package ch.epfl.bluebrain.nexus.service.kafka

import java.util.concurrent.TimeUnit.SECONDS

import akka.actor.Actor.emptyBehavior
import akka.actor._
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset, CommittableOffsetBatch}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.persistence.query.{NoOffset, Sequence}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexFailuresLog
import ch.epfl.bluebrain.nexus.service.indexer.retryer.syntax._
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.Backoff
import io.circe._
import io.circe.parser._
import journal.Logger
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Do not instantiate this class directly, see [[KafkaConsumer.start]].
  */
private[kafka] class KafkaSupervisedConsumer[Event](settings: ConsumerSettings[String, String],
                                                    index: Event => Future[Unit],
                                                    topic: String,
                                                    decoder: Decoder[Event],
                                                    committable: Boolean,
                                                    failuresLog: Option[IndexFailuresLog])
    extends Actor {

  private implicit val as: ActorSystem       = context.system
  private implicit val ec: ExecutionContext  = as.dispatcher
  private implicit val mt: ActorMaterializer = ActorMaterializer()

  private val log     = Logger[this.type]
  private val config  = context.system.settings.config.getConfig("indexing.retry")
  private val retries = config.getInt("max-count")
  private val strategy =
    Backoff(Duration(config.getDuration("max-duration", SECONDS), SECONDS), config.getDouble("random-factor"))

  override val receive: Receive = emptyBehavior

  override def preStart(): Unit = {
    super.preStart()
    val done = if (committable) {
      Consumer
        .committableSource(settings, Subscriptions.topics(topic))
        .mapAsync(1) {
          case msg @ CommittableMessage(record, offset) =>
            process(record, Some(offset))
              .recover { case e => log.error(s"Unexpected failure while processing message: $msg", e) }
              .map(_ => offset)
        }
        .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
          batch.updated(elem)
        }
        .mapAsync(1)(_.commitScaladsl())
        .runWith(Sink.ignore)
    } else {
      Consumer
        .plainSource(settings, Subscriptions.topics(topic))
        .mapAsync(1) { record =>
          process(record, None)
            .recover { case e => log.error(s"Unexpected failure while processing record: $record", e) }
        }
        .runWith(Sink.ignore)
    }

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

  private def process(record: ConsumerRecord[String, String], offset: Option[CommittableOffset]): Future[Unit] = {
    parse(record.value).flatMap(decoder.decodeJson) match {
      case Right(event) =>
        log.debug(s"Received message: ${record.value}")
        (() => index(event))
          .retry(retries)(strategy)
          .recoverWith {
            // $COVERAGE-OFF$
            case e =>
              log.error(s"Failed to index event: $event; skipping.", e)
              storeFailure(record, offset)
          }
      case Left(e) =>
        log.error(s"Failed to decode message: ${record.value}; skipping.", e)
        storeFailure(record, offset)
      // $COVERAGE-ON$
    }
  }

  // $COVERAGE-OFF$
  private def storeFailure(record: ConsumerRecord[String, String], offset: Option[CommittableOffset]): Future[Unit] =
    failuresLog match {
      case None => Future.successful(())
      case Some(ifl) =>
        val off = offset match {
          case None                    => NoOffset
          case Some(committableOffset) => Sequence(committableOffset.partitionOffset.offset)
        }
        ifl.storeEvent(record.key, off, record.value)
    }
  // $COVERAGE-ON$

}
