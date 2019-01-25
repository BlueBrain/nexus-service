package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexerConfig._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer.{Graph, OffsetEvts}
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.{Backoff, Linear}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class IndexerConfigSpec extends TestKit(ActorSystem("IndexerConfigSpec")) with WordSpecLike with Matchers {

  import system.dispatcher

  private[persistence] def toFlow(index: List[String] => Future[Unit]): Graph[String] = {
    Flow[OffsetEvts[String]].mapAsync(1) {
      case OffsetEvts(off, events) => index(events.map(_.value)).map(_ => off)
    }
  }

  "A IndexerConfig" should {
    val indexF: List[String] => Future[Unit] = (_: List[String]) => Future(())
    val initF: () => Future[Unit]            = () => Future(())

    "build a the configuration for index function with persistence" in {
      val storage = Persist(restart = false)
      val expected =
        IndexConfigFunction("t", "p", "n", indexF, initF, 1, 50 millis, 1, Linear(0 seconds), storage)
      builder.name("n").plugin("p").tag("t").index(indexF).init(initF).build shouldEqual expected
    }

    "build a the configuration for index function without persistence" in {
      val expected =
        IndexConfigFunction("t", "p", "n", indexF, initF, 5, 100 millis, 1, Linear(1 seconds), Volatile)
      builder
        .name("n")
        .plugin("p")
        .tag("t")
        .batch(5, 100 millis)
        .retry(1, Linear(1 seconds))
        .index(indexF)
        .init(initF)
        .offset(Volatile)
        .build shouldEqual expected
    }

    "build a the configuration for flow with persistence" in {
      val storage  = Persist(restart = false)
      val flowF    = toFlow(indexF)
      val expected = IndexConfigFlow("t", "p", "n", flowF, initF, 1, 50 millis, 1, Linear(0 seconds), storage)
      builder.name("n").plugin("p").tag("t").flow(flowF).init(initF).build shouldEqual expected
    }

    "build a the configuration for flow with persistence and restart" in {
      val storage  = Persist(restart = true)
      val flowF    = toFlow(indexF)
      val expected = IndexConfigFlow("t", "p", "n", flowF, initF, 1, 50 millis, 1, Linear(0 seconds), storage)
      builder.name("n").plugin("p").tag("t").restart(true).flow(flowF).init(initF).build shouldEqual expected
    }

    "build a the configuration for flow without persistence" in {
      val flowF    = toFlow(indexF)
      val expected = IndexConfigFlow("t", "p", "n", flowF, initF, 1, 50 millis, 1, Linear(0 seconds), Volatile)
      builder.name("n").plugin("p").tag("t").flow(flowF).init(initF).offset(Volatile).build shouldEqual expected
    }

    "build from config" in {
      val storage = Persist(restart = false)
      val expected =
        IndexConfigFunction("t", "p", "n", indexF, initF, 1, 40 millis, 3, Backoff(5 seconds, 0.0), storage)
      fromConfig.name("n").plugin("p").tag("t").index(indexF).init(initF).build shouldEqual expected
    }
  }
}
