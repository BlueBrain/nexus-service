package ch.epfl.bluebrain.nexus.service.indexer.persistence

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.cluster.Cluster
import akka.stream.ActorMaterializer
import akka.testkit.{TestActorRef, TestKit, TestKitBase}
import ch.epfl.bluebrain.nexus.commons.types.{Err, RetriableErr}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.Fixture.{RetryExecuted, _}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexerConfig.fromConfig
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexerSpec._
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator.Stop
import ch.epfl.bluebrain.nexus.sourcing.akka.{ShardingAggregate, SourcingAkkaSettings}
import io.circe.generic.auto._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

//noinspection TypeAnnotation
@DoNotDiscover
class SequentialTagIndexerSpec
    extends TestKitBase
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with Eventually {

  implicit lazy val system = SystemBuilder.cluster("SequentialTagIndexerSpec")
  implicit val ec          = system.dispatcher
  implicit val mt          = ActorMaterializer()

  private val cluster = Cluster(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cluster.join(cluster.selfAddress)
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(30 seconds, 1 second)

  "A SequentialTagIndexer" should {
    val pluginId         = "cassandra-query-journal"
    val sourcingSettings = SourcingAkkaSettings(journalPluginId = pluginId)

    def initFunction(init: AtomicLong): () => Future[Unit] =
      () => {
        init.incrementAndGet()
        Future.successful(())
      }

    sealed abstract class Context[T](name: String, batch: Int = 1) {
      val agg = ShardingAggregate(name, sourcingSettings)(Fixture.initial, Fixture.next, Fixture.eval)

      val count = new AtomicLong(0L)
      val init  = new AtomicLong(10L)
      val index = (l: List[T]) =>
        Future.successful[Unit] {
          l.size shouldEqual batch
          val _ = count.incrementAndGet()
      }

      val projId = UUID.randomUUID().toString
    }

    "index existing events" in new Context[Event]("agg") {
      agg.append("first", Fixture.Executed).futureValue

      val config  = fromConfig.name(projId).plugin(pluginId).tag("executed").init(initFunction(init)).index(index).build
      val indexer = TestActorRef(new StreamCoordinator(config.prepareInit, config.source))

      eventually {
        count.get() shouldEqual 1L
        init.get shouldEqual 11L
      }

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }

    "recover from temporary failures on init function" in new Context[Event]("something") {

      val initCalled = new AtomicLong(0L)

      def initFail(init: AtomicLong): () => Future[Unit] =
        () => {
          if (initCalled.compareAndSet(0L, 1L) || initCalled.compareAndSet(1L, 2L))
            Future.failed(new RuntimeException)
          else {
            init.incrementAndGet()
            Future.successful(())
          }
        }

      agg.append("a", Fixture.YetAnotherExecuted).futureValue

      val config  = fromConfig.name(projId).plugin(pluginId).tag("yetanother").index(index).init(initFail(init)).build
      val indexer = TestActorRef(new StreamCoordinator(config.prepareInit, config.source))

      eventually {
        initCalled.get() shouldEqual 2L
        init.get shouldEqual 11L
        count.get() shouldEqual 1L
      }

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }

    "select only the configured event types" in new Context[Event](name = "selected", batch = 2) {
      agg.append("first", Fixture.Executed).futureValue
      agg.append("second", Fixture.Executed).futureValue
      agg.append("third", Fixture.Executed).futureValue
      agg.append("selected1", Fixture.OtherExecuted).futureValue
      agg.append("selected2", Fixture.OtherExecuted).futureValue
      agg.append("selected3", Fixture.OtherExecuted).futureValue
      agg.append("selected4", Fixture.OtherExecuted).futureValue

      val config =
        fromConfig.name(projId).plugin(pluginId).tag("other").index(index).batch(2).init(initFunction(init)).build
      val indexer = TestActorRef(new StreamCoordinator(config.prepareInit, config.source))

      eventually {
        count.get() shouldEqual 2L
        init.get shouldEqual 11L
      }

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }

    "restart the indexing if the Done is emitted" in new Context[Event]("agg2") {
      agg.append("first", Fixture.AnotherExecuted).futureValue

      val config  = fromConfig.name(projId).plugin(pluginId).tag("another").index(index).init(initFunction(init)).build
      val indexer = TestActorRef(new StreamCoordinator(config.prepareInit, config.source))

      eventually {
        count.get() shouldEqual 1L
        init.get shouldEqual 11L
      }
      indexer ! Done

      agg.append("second", Fixture.AnotherExecuted).futureValue

      eventually {
        count.get() shouldEqual 2L
        init.get shouldEqual 12L
      }

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }

    "retry when index function fails" in new Context[RetryExecuted.type]("retry") {
      agg.append("retry", Fixture.RetryExecuted).futureValue

      override val index = (_: List[RetryExecuted.type]) => Future.failed[Unit](SomeError(count.incrementAndGet()))

      val config  = fromConfig.name(projId).plugin(pluginId).tag("retry").index(index).init(initFunction(init)).build
      val indexer = TestActorRef(new StreamCoordinator(config.prepareInit, config.source))

      eventually {
        count.get() shouldEqual 4
        init.get shouldEqual 11L
      }
      eventually {
        IndexFailuresLog(projId)
          .fetchEvents[RetryExecuted.type]
          .runFold(Vector.empty[RetryExecuted.type])(_ :+ _)
          .futureValue shouldEqual List(RetryExecuted)
      }

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }

    "not retry when index function fails with a non RetriableErr" in new Context[IgnoreExecuted.type]("ignore") {
      agg.append("ignore", Fixture.IgnoreExecuted).futureValue

      override val index =
        (_: List[IgnoreExecuted.type]) => Future.failed[Unit](SomeOtherError(count.incrementAndGet()))

      val config  = fromConfig.name(projId).plugin(pluginId).tag("ignore").index(index).init(initFunction(init)).build
      val indexer = TestActorRef(new StreamCoordinator(config.prepareInit, config.source))

      eventually {
        count.get() shouldEqual 1L
        init.get shouldEqual 11L
      }

      IndexFailuresLog(projId)
        .fetchEvents[IgnoreExecuted.type]
        .runFold(Vector.empty[IgnoreExecuted.type])(_ :+ _)
        .futureValue shouldEqual List(IgnoreExecuted)

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }
  }

}

object SequentialTagIndexerSpec {
  case class SomeError(count: Long)      extends RetriableErr("some error")
  case class SomeOtherError(count: Long) extends Err("some OTHER error")

}
