package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexerConfig._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer.{Graph, OffsetEvts}
import ch.epfl.bluebrain.nexus.sourcing.akka.Retry
import ch.epfl.bluebrain.nexus.sourcing.akka.RetryStrategy.{Backoff, Linear}
import monix.eval.Task
import org.scalatest.{Matchers, WordSpecLike}
import monix.execution.Scheduler.Implicits.global
import org.scalactic.Equality

import scala.concurrent.duration._

class IndexerConfigSpec extends TestKit(ActorSystem("IndexerConfigSpec")) with WordSpecLike with Matchers {

  private[persistence] def toFlow(index: List[String] => Task[Unit]): Graph[String] = {
    Flow[OffsetEvts[String]].mapAsync(1) {
      case OffsetEvts(off, events) => index(events.map(_.value)).map(_ => off).runToFuture
    }
  }

  "A IndexerConfig" should {
    val indexF: List[String] => Task[Unit] = (_: List[String]) => Task.unit
    val initF: Task[Unit]                  = Task.unit
    val strategy                           = Linear(0 millis, 2000 hours)

    implicit def eqIndexerConfig[T]: Equality[IndexerConfig[String, Throwable, T]] =
      (a: IndexerConfig[String, Throwable, T], b: Any) => {
        val that = b.asInstanceOf[IndexerConfig[String, Throwable, T]]
        a.pluginId == that.pluginId && a.batchTo == that.batchTo && a.batch == that.batch && a.init == that.init && a.storage == that.storage && a.tag == that.tag
      }

    "build a the configuration for index function with persistence" in {
      val storage = Persist(restart = false)
      val expected =
        IndexConfigFunction("t", "p", "n", indexF, initF, 1, 50 millis, Retry(strategy), storage)
      builder.name("n").plugin("p").tag("t").index(indexF).init(initF).build shouldEqual expected
    }

    "build a the configuration for index function without persistence" in {
      val st = Linear(10 millis, 1 hour)
      val expected =
        IndexConfigFunction("t", "p", "n", indexF, initF, 5, 100 millis, Retry(st), Volatile)
      builder
        .name("n")
        .plugin("p")
        .tag("t")
        .batch(5, 100 millis)
        .retry(st)
        .index(indexF)
        .init(initF)
        .offset(Volatile)
        .build shouldEqual expected
    }

    "build a the configuration for flow with persistence" in {
      val storage  = Persist(restart = false)
      val flowF    = toFlow(indexF)
      val expected = IndexConfigFlow("t", "p", "n", flowF, initF, 1, 50 millis, Retry(strategy), storage)
      builder.name("n").plugin("p").tag("t").flow(flowF).init(initF).build shouldEqual expected
    }

    "build a the configuration for flow with persistence and restart" in {
      val storage  = Persist(restart = true)
      val flowF    = toFlow(indexF)
      val expected = IndexConfigFlow("t", "p", "n", flowF, initF, 1, 50 millis, Retry(strategy), storage)
      builder.name("n").plugin("p").tag("t").restart(true).flow(flowF).init(initF).build shouldEqual expected
    }

    "build a the configuration for flow without persistence" in {
      val flowF    = toFlow(indexF)
      val expected = IndexConfigFlow("t", "p", "n", flowF, initF, 1, 50 millis, Retry(strategy), Volatile)
      builder.name("n").plugin("p").tag("t").flow(flowF).init(initF).offset(Volatile).build shouldEqual expected
    }

    "build from config" in {
      val storage  = Persist(restart = false)
      val st       = Backoff(100 millis, 10 hours, 0.5, 7)
      val expected = IndexConfigFunction("t", "p", "n", indexF, initF, 10, 40 millis, Retry(st), storage)
      fromConfig.name("n").plugin("p").tag("t").index(indexF).init(initF).build shouldEqual expected
    }
  }
}
