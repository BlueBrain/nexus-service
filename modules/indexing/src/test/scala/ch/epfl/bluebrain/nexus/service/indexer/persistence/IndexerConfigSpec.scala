package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.actor.ActorSystem
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexerConfig._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.sourcing.akka.{Retry, RetryStrategy}
import ch.epfl.bluebrain.nexus.sourcing.akka.RetryStrategy.{Backoff, Linear}
import monix.eval.Task
import org.scalactic.Equality
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class IndexerConfigSpec extends TestKit(ActorSystem("IndexerConfigSpec")) with WordSpecLike with Matchers {

  "A IndexerConfig" should {
    val indexF: List[String] => Task[Unit]       = _ => Task.unit
    val initF: Task[Unit]                        = Task.unit
    val strategy: RetryStrategy                  = Linear(0 millis, 2000 hours)
    val identity: String => Task[Option[String]] = (v: String) => Task.pure(Some(v))

    implicit def eqIndexerConfig[T <: OffsetStorage]: Equality[IndexerConfig[Task, String, String, Throwable, T]] =
      (a: IndexerConfig[Task, String, String, Throwable, T], b: Any) => {
        val that = b.asInstanceOf[IndexerConfig[Task, String, String, Throwable, T]]
        a.pluginId == that.pluginId && a.batchTo == that.batchTo && a.batch == that.batch && a.init == that.init && a.storage == that.storage && a.tag == that.tag && that.index == a.index && that.mapping == a.mapping
      }

    "build a the configuration for index function with persistence" in {
      val storage = Persist(restart = false)
      val expected: IndexerConfig[Task, String, String, Throwable, Persist] =
        IndexerConfig("t", "p", "n", identity, indexF, initF, 1, 50 millis, Retry(strategy), storage)
      builder[Task]
        .name("n")
        .plugin("p")
        .tag("t")
        .mapping(identity)
        .index(indexF)
        .init(initF)
        .build shouldEqual expected
    }

    "build a the configuration for index function without persistence" in {
      val st = Linear(10 millis, 1 hour)
      val expected: IndexerConfig[Task, String, String, Throwable, Volatile] =
        IndexerConfig("t", "p", "n", identity, indexF, initF, 5, 100 millis, Retry(st), Volatile)
      builder[Task]
        .name("n")
        .plugin("p")
        .tag("t")
        .batch(5, 100 millis)
        .retry(st)
        .mapping(identity)
        .index(indexF)
        .init(initF)
        .offset(Volatile)
        .build shouldEqual expected
    }

    "build from config" in {
      val storage = Persist(restart = false)
      val st      = Backoff(100 millis, 10 hours, 0.5, 7)
      val expected: IndexerConfig[Task, String, String, Throwable, Persist] =
        IndexerConfig("t", "p", "n", identity, indexF, initF, 10, 40 millis, Retry(st), storage)
      fromConfig[Task]
        .name("n")
        .plugin("p")
        .tag("t")
        .mapping(identity)
        .index(indexF)
        .init(initF)
        .build shouldEqual expected
    }
  }
}
