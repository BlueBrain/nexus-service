package ch.epfl.bluebrain.nexus.service.queue

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import io.circe.Decoder
import journal.Logger
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class KafkaConsumerSpec
    extends TestKit(ActorSystem("embedded-kafka-consumer"))
    with WordSpecLike
    with Eventually
    with Matchers
    with BeforeAndAfterAll
    with EmbeddedKafka {

  override implicit val patienceConfig: PatienceConfig  = PatienceConfig(30.seconds, 3.seconds)
  private implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

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

      val message  = KafkaEvent("http://localhost/context", "some-id", 42L)
      val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
          .withGroupId("group-id-1")
      val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)

      withRunningKafka {
        val kafkaProducer = producerSettings.createKafkaProducer()
        val kafkaPublisher = new KafkaPublisher[KafkaEvent](kafkaProducer, "test-topic-1")

        for (_ <- 1 to 100) {
          kafkaPublisher.send(message)
        }
        val supervisor = KafkaConsumer.start[KafkaEvent](consumerSettings, index, "test-topic-1")
        eventually {
          counter.get shouldEqual 100
        }
        blockingStop(supervisor)
        kafkaProducer.close()
      }
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
      withRunningKafka {
        for (_ <- 1 to 100) {
          publishStringMessageToKafka("test-topic-2", """{"msg":"foo"}""")
        }
        val supervisor = KafkaConsumer.start[Message](consumerSettings, index, "test-topic-2")
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

      val as1   = ActorSystem("embedded-kafka-1")
      val as2   = ActorSystem("embedded-kafka-2")
      val topic = "test-topic-3"

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group-id-3")
      implicit val serializer: StringSerializer = new StringSerializer
      withRunningKafka {
        for (_ <- 1 to 100) {
          publishToKafka(topic, "partition-1", """{"msg":"foo"}""")
          publishToKafka(topic, "partition-2", """{"msg":"bar"}""")
        }

        val supervisor1 = KafkaConsumer.start[Message](consumerSettings, index, topic)(as1, msgDecoder)
        val supervisor2 = KafkaConsumer.start[Message](consumerSettings, index, topic)(as2, msgDecoder)

        eventually {
          counter1.get shouldEqual 100
          counter2.get shouldEqual 100
        }
        blockingStop(supervisor1)
        blockingStop(supervisor2)
      }
      TestKit.shutdownActorSystem(as1)
      TestKit.shutdownActorSystem(as2)
    }

    "handle exceptions while processing messages" in {
      val counter = new AtomicInteger

      def isEven(msg: Message): Future[Unit] = {
        if (counter.incrementAndGet() % 2 == 0)
          Future.successful(())
        else
          Future.failed(new RetriableErr(s"We need to use $msg somewhere!"))
      }

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group-id-4")
      withRunningKafka {
        for (i <- 1 to 3) {
          publishStringMessageToKafka("test-topic-4", s"""{"msg":"foo-$i"}""")
        }
        val supervisor = KafkaConsumer.start[Message](consumerSettings, isEven, "test-topic-4")
        eventually {
          counter.get shouldEqual 6
        }
        blockingStop(supervisor)
      }

    }
  }

  private def blockingStop(actor: ActorRef): Unit = {
    watch(actor)
    system.stop(actor)
    expectTerminated(actor, 30.seconds)
    log.warn("Actor stopped")
    val _ = unwatch(actor)
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

}
