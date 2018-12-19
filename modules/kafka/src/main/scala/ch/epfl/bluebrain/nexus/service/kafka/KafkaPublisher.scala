package ch.epfl.bluebrain.nexus.service.kafka

import java.util.concurrent.Future

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ProducerMessage._
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Flow, Source}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer.{Graph, OffsetEvts}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.{IndexerConfig, SequentialTagIndexer}
import ch.epfl.bluebrain.nexus.service.kafka.key._
import io.circe.Encoder
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import shapeless.Typeable

/**
  * Helper class to send messages to different Kafka topics sharing only one instance
  * of a [[org.apache.kafka.clients.producer.KafkaProducer]]
  *
  * @param producer the shared [[KafkaProducer]] instance
  * @param topic    the kafka topic
  * @tparam Event   the generic event type
  */
class KafkaPublisher[Event: Encoder: Key](producer: KafkaProducer[String, String], topic: String) {

  /**
    * Manually sends a single message
    *
    * @param event the event to send
    * @return the metadata for the record acknowledge by the server
    */
  def send(event: Event): Future[RecordMetadata] = {
    val message = new ProducerRecord[String, String](topic, event.toKey, event.asJson.noSpaces)
    producer.send(message)
  }
}

object KafkaPublisher {

  private def flow[Event: Encoder: Key](producerSettings: ProducerSettings[String, String],
                                        topic: String): Graph[Event] = {
    Flow[OffsetEvts[Event]]
      .map {
        case OffsetEvts(off, events) =>
          events.map { event =>
            Message(new ProducerRecord[String, String](topic, event.value.toKey, event.value.asJson.noSpaces), off)
          }
      }
      .flatMapConcat(Source.apply)
      .via(Producer.flexiFlow(producerSettings))
      .map(_.passThrough)

  }
  // $COVERAGE-OFF$
  /**
    * Starts publishing events to Kafka using a [[ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer]]
    *
    * @param pluginId query plugin ID
    * @param tag events with which tag to publish
    * @param name name of the actor
    * @param producerSettings Akka StreamsKafka producer settings
    * @param topic topic to publish to
    * @param as implicit ActorSystem
    * @tparam Event the generic event type
    * @return ActorRef for the started actor
    */
  final def startTagStream[Event: Encoder: Key: Typeable](pluginId: String,
                                                          tag: String,
                                                          name: String,
                                                          producerSettings: ProducerSettings[String, String],
                                                          topic: String)(implicit as: ActorSystem): ActorRef = {
    val config = IndexerConfig.builder.plugin(pluginId).name(name).tag(tag).flow(flow(producerSettings, topic)).build
    SequentialTagIndexer.start(config)
  }
  // $COVERAGE-ON$
}
