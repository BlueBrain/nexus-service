package ch.epfl.bluebrain.nexus.service.kafka

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.persistence.query.Offset
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexFailuresLog
import io.circe.{Decoder, Encoder}
import journal.Logger
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class KafkaConsumerSpec
    extends TestKit(ActorSystem("embedded-kafka-consumer"))
    with WordSpecLike
    with Eventually
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with EmbeddedKafka {

  override implicit val patienceConfig: PatienceConfig  = PatienceConfig(30.seconds, 3.seconds)
  private implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    ()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    EmbeddedKafka.stop()
  }

  private val log = Logger[this.type]

  case class Message(msg: String)

  private implicit val msgDecoder: Decoder[Message] =
    Decoder.instance(_.downField("msg").as[String].map(Message.apply))

  "KafkaConsumer" should {

    "decode and index received messages" in {
      val counter = new AtomicInteger

      def index(e: KafkaEvent): Future[Unit] = {
        e.`@context` shouldEqual "http://localhost/context"
        e._id shouldEqual "some-id"
        e._rev shouldEqual 42L
        counter.incrementAndGet()
        Future.successful(())
      }

      val message = KafkaEvent("http://localhost/context", "some-id", 42L)
      val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
        .withGroupId("group-id-1")
      val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)

      val kafkaProducer  = producerSettings.createKafkaProducer()
      val kafkaPublisher = new KafkaPublisher[KafkaEvent](kafkaProducer, "test-topic-1")

      for (_ <- 1 to 100) {
        kafkaPublisher.send(message)
      }
      val supervisor = KafkaConsumer.start[KafkaEvent](consumerSettings, index, "test-topic-1", "one")
      eventually {
        counter.get shouldEqual 100
      }
      blockingStop(supervisor)
      kafkaProducer.close()
    }

    "restart from last committed offset" in {
      val counter = new AtomicInteger

      def index(msg: Message): Future[Unit] = {
        val expected =
          if (counter.get < 100) Message("foo")
          else Message("bar")

        if (msg == expected) {
          counter.incrementAndGet()
          Future.successful(())
        } else {
          Future.failed(new Exception)
        }
      }

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group-id-2")
      for (_ <- 1 to 100) {
        publishStringMessageToKafka("test-topic-2", """{"msg":"foo"}""")
      }
      val supervisor = KafkaConsumer.start[Message](consumerSettings, index, "test-topic-2", "two")
      eventually {
        counter.get shouldEqual 100
      }
      KafkaConsumer.stop(supervisor)

      for (_ <- 1 to 100) {
        publishStringMessageToKafka("test-topic-2", """{"msg":"bar"}""")
      }
      eventually {
        counter.get shouldEqual 200
      }
      blockingStop(supervisor)
    }

    "run in parallel" in {
      val counter1 = new AtomicInteger
      val counter2 = new AtomicInteger

      def index(msg: Message): Future[Unit] = msg match {
        case Message("foo") =>
          counter1.incrementAndGet()
          Future.successful(())
        case Message("bar") =>
          counter2.incrementAndGet()
          Future.successful(())
        case _ =>
          Future.failed(new Exception)
      }

      val topic = "test-topic-3"

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group-id-3")
      implicit val serializer: StringSerializer = new StringSerializer
      for (_ <- 1 to 100) {
        publishToKafka(topic, "partition-1", """{"msg":"foo"}""")
        publishToKafka(topic, "partition-2", """{"msg":"bar"}""")
      }

      val supervisor1 =
        KafkaConsumer.start[Message](consumerSettings, index, topic, "three-1", committable = true)(system, msgDecoder)
      val supervisor2 =
        KafkaConsumer.start[Message](consumerSettings, index, topic, "three-2", committable = false)(system, msgDecoder)

      eventually {
        counter1.get shouldEqual 100
        counter2.get shouldEqual 100
      }
      blockingStop(supervisor1)
      blockingStop(supervisor2)
    }

    "handle exceptions while processing messages" in {
      val counter  = new AtomicInteger
      val failures = new AtomicInteger

      def index(msg: Message): Future[Unit] = {
        counter.incrementAndGet() match {
          case 1 => Future.successful(())
          case 2 => Future.failed(new RetriableErr(s"We need to use $msg somewhere!"))
          case 3 => Future.successful(())
          case 4 => Future.failed(new RuntimeException("Thrown by the indexing function"))
          case _ => fail()
        }
      }

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group-id-4")
      val failuresLog = new IndexFailuresLog {
        override def identifier: String = "mocked-index-failures-log"

        override def storeEvent[T](persistenceId: String,
                                   offset: Offset,
                                   event: T)(implicit E: Encoder[T]): Future[Unit] = {
          failures.incrementAndGet()
          Future.successful(())
        }

        override def fetchEvents[T](implicit D: Decoder[T]): Source[T, _] = ???
      }
      for (i <- 1 to 3) {
        publishStringMessageToKafka("test-topic-4", s"""{"msg":"foo-$i"}""")
      }
      val supervisor =
        KafkaConsumer.start[Message](consumerSettings, index, "test-topic-4", "four", true, Some(failuresLog))
      eventually {
        counter.get shouldEqual 4
        failures.get shouldEqual 1
      }
      blockingStop(supervisor)

    }
  }

  private def blockingStop(actor: ActorRef): Unit = {
    watch(actor)
    system.stop(actor)
    expectTerminated(actor, 30.seconds)
    log.warn("Actor stopped")
    val _ = unwatch(actor)
  }
}
