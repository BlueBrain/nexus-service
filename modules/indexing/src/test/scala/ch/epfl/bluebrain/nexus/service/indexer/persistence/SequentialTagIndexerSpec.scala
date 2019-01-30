package ch.epfl.bluebrain.nexus.service.indexer.persistence

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.cluster.Cluster
import akka.stream.ActorMaterializer
import akka.testkit.{TestActorRef, TestKit, TestKitBase}
import akka.util.Timeout
import cats.MonadError
import cats.effect.{IO, Timer}
import ch.epfl.bluebrain.nexus.commons.types.{Err, RetriableErr}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.Fixture._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexerConfig.fromConfig
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexerSpec._
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator.Stop
import ch.epfl.bluebrain.nexus.sourcing.akka.RetryStrategy.Linear
import ch.epfl.bluebrain.nexus.sourcing.akka._
import io.circe.generic.auto._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

//noinspection TypeAnnotation
@DoNotDiscover
class SequentialTagIndexerSpec
    extends TestKitBase
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with Eventually {

  implicit lazy val system              = SystemBuilder.cluster("SequentialTagIndexerSpec")
  implicit val ec                       = system.dispatcher
  implicit val mt                       = ActorMaterializer()
  private implicit val timer: Timer[IO] = IO.timer(ec)

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
    PatienceConfig(35 seconds, 500 millis)

  "A SequentialTagIndexer" should {
    val pluginId = "cassandra-query-journal"
    val config = AkkaSourcingConfig(
      Timeout(30.second),
      pluginId,
      200.milliseconds,
      ExecutionContext.global
    )

    implicit val F: MonadError[Task, RetriableErr] = new MonadError[Task, RetriableErr] {

      override def handleErrorWith[A](fa: Task[A])(f: RetriableErr => Task[A]): Task[A] = fa.onErrorRecoverWith {
        case t: RetriableErr => f(t)
      }
      override def raiseError[A](e: RetriableErr): Task[A]                   = Task.raiseError(e)
      override def pure[A](x: A): Task[A]                                    = Task.pure(x)
      override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B]      = fa.flatMap(f)
      override def tailRecM[A, B](a: A)(f: A => Task[Either[A, B]]): Task[B] = Task.tailRecM(a)(f)
    }

    def initFunction(init: AtomicLong): Task[Unit] =
      Task.deferFuture {
        init.incrementAndGet()
        Future.successful(())
      }

    sealed abstract class Context[T](name: String, batch: Int = 1) {
      val agg = AkkaAggregate
        .sharded[IO](
          name,
          Fixture.initial,
          Fixture.next,
          Fixture.eval,
          PassivationStrategy.immediately[State, Cmd],
          Retry[IO, Throwable](RetryStrategy.Never),
          config,
          shards = 10
        )
        .unsafeRunSync()

      val count = new AtomicLong(0L)
      val init  = new AtomicLong(10L)
      val index: List[T] => Task[Unit] = (l: List[T]) =>
        Task.deferFuture(Future.successful[Unit] {
          l.size shouldEqual batch
          val _ = count.incrementAndGet()
        })

      val projId = UUID.randomUUID().toString
    }

    "index existing events" in new Context[Event]("agg") {
      agg.append("first", Fixture.Executed).unsafeRunAsyncAndForget()

      val config = fromConfig
        .name(projId)
        .plugin(pluginId)
        .tag("executed")
        .init(initFunction(init))
        .index(index)
        .retry[RetriableErr](Linear(100 millis, 1 second, maxRetries = 3))
        .build
      val builder = new PersistentSourceBuilder(config)
      val indexer = TestActorRef(new StreamCoordinator(builder.prepareInit, builder.source))

      eventually {
        count.get shouldEqual 1L
        init.get shouldEqual 11L
      }

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }

    "recover from temporary failures on init function" in new Context[Event]("something") {

      val initCalled = new AtomicLong(0L)

      def initFail(init: AtomicLong): Task[Unit] =
        Task.deferFuture {
          if (initCalled.compareAndSet(0L, 1L) || initCalled.compareAndSet(1L, 2L))
            Future.failed(new RetriableErr("recoverable error"))
          else {
            init.incrementAndGet()
            Future.successful(())
          }
        }

      agg.append("a", Fixture.YetAnotherExecuted).unsafeRunAsyncAndForget()

      val config = fromConfig
        .name(projId)
        .plugin(pluginId)
        .tag("yetanother")
        .index(index)
        .init(initFail(init))
        .retry[RetriableErr](Linear(100 millis, 1 second, maxRetries = 3))
        .build
      val builder = new PersistentSourceBuilder(config)
      val indexer = TestActorRef(new StreamCoordinator(builder.prepareInit, builder.source))

      eventually {
        initCalled.get shouldEqual 2L
        init.get shouldEqual 11L
        count.get shouldEqual 1L
      }

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }

    "select only the configured event types" in new Context[Event](name = "selected", batch = 2) {
      agg.append("first", Fixture.Executed).unsafeRunAsyncAndForget()
      agg.append("second", Fixture.Executed).unsafeRunAsyncAndForget()
      agg.append("third", Fixture.Executed).unsafeRunAsyncAndForget()
      agg.append("selected1", Fixture.OtherExecuted).unsafeRunAsyncAndForget()
      agg.append("selected2", Fixture.OtherExecuted).unsafeRunAsyncAndForget()
      agg.append("selected3", Fixture.OtherExecuted).unsafeRunAsyncAndForget()
      agg.append("selected4", Fixture.OtherExecuted).unsafeRunAsyncAndForget()

      val config =
        fromConfig
          .name(projId)
          .plugin(pluginId)
          .tag("other")
          .index(index)
          .batch(2)
          .init(initFunction(init))
          .retry[RetriableErr](Linear(100 millis, 1 second, maxRetries = 3))
          .build
      val builder = new PersistentSourceBuilder(config)
      val indexer = TestActorRef(new StreamCoordinator(builder.prepareInit, builder.source))

      eventually {
        count.get shouldEqual 2L
        init.get shouldEqual 11L
      }

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }

    "restart the indexing if the Done is emitted" in new Context[Event]("agg2") {
      agg.append("first", Fixture.AnotherExecuted).unsafeRunAsyncAndForget()

      val config = fromConfig
        .name(projId)
        .plugin(pluginId)
        .tag("another")
        .index(index)
        .init(initFunction(init))
        .retry[RetriableErr](Linear(100 millis, 1 second, maxRetries = 3))
        .build
      val builder = new PersistentSourceBuilder(config)
      val indexer = TestActorRef(new StreamCoordinator(builder.prepareInit, builder.source))

      eventually {
        count.get shouldEqual 1L
        init.get shouldEqual 11L
      }
      indexer ! Done

      agg.append("second", Fixture.AnotherExecuted).unsafeRunAsyncAndForget()

      eventually {
        count.get shouldEqual 2L
        init.get shouldEqual 12L
      }

      watch(indexer)
      indexer ! Stop
      expectTerminated(indexer)
    }

    "retry when index function fails" in new Context[RetryExecuted.type]("retry") {
      agg.append("retry", Fixture.RetryExecuted).unsafeRunAsyncAndForget()

      override val index =
        (_: List[RetryExecuted.type]) => Task.deferFuture(Future.failed[Unit](SomeError(count.incrementAndGet())))

      val config = fromConfig
        .name(projId)
        .plugin(pluginId)
        .tag("retry")
        .index(index)
        .init(initFunction(init))
        .retry[RetriableErr](Linear(100 millis, 1 second, maxRetries = 3))
        .build
      val builder = new PersistentSourceBuilder(config)
      val indexer = TestActorRef(new StreamCoordinator(builder.prepareInit, builder.source))

      eventually {
        count.get shouldEqual 4
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
      agg.append("ignore", Fixture.IgnoreExecuted).unsafeRunAsyncAndForget()

      override val index =
        (_: List[IgnoreExecuted.type]) => Task.deferFuture(Future.failed(SomeOtherError(count.incrementAndGet())))

      val config = fromConfig
        .name(projId)
        .plugin(pluginId)
        .tag("ignore")
        .index(index)
        .init(initFunction(init))
        .retry[RetriableErr](Linear(100 millis, 1 second, maxRetries = 3))
        .build

      val builder = new PersistentSourceBuilder(config)
      val indexer = TestActorRef(new StreamCoordinator(builder.prepareInit, builder.source))

      eventually {
        count.get shouldEqual 1L
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
