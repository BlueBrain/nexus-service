package ch.epfl.bluebrain.nexus.service.indexer.cache

import cats.effect.IO
import cats.effect.IO.timer
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreSpec._
import ch.epfl.bluebrain.nexus.service.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.RetryStrategyConfig
import org.scalatest.Matchers

import scala.concurrent.duration._

class KeyValueStoreSpec
    extends ActorSystemFixture("KeyValueStoreSpec", true)
    with Matchers
    with IOEitherValues
    with IOOptionValues {

  private implicit val ec = system.dispatcher

  private implicit val t = timer(ec)

  "A KeyValueStore" should {

    implicit val config = KeyValueStoreConfig(4 seconds, 3 seconds, RetryStrategyConfig("never", 0 seconds, 0, 0))
    val store           = KeyValueStore.distributed[IO, String, RevisionedValue[String]]("spec", { case (_, rv) => rv.rev })

    "store values" in {
      store.put("a", RevisionedValue(1, "a")).ioValue
      store.get("a").some shouldEqual RevisionedValue(1, "a")
    }

    "update values" in {
      store.put("a", RevisionedValue(2, "aa")).ioValue
      store.get("a").some shouldEqual RevisionedValue(2, "aa")
    }

    "discard updates for previous revisions" in {
      store.put("a", RevisionedValue(1, "a")).ioValue
      store.get("a").some shouldEqual RevisionedValue(2, "aa")
    }

    "discard updates for same revisions" in {
      store.put("a", RevisionedValue(2, "b")).ioValue
      store.get("a").some shouldEqual RevisionedValue(2, "aa")
    }

    "discard updates on present keys" in {
      store.putIfAbsent("a", RevisionedValue(4, "b")).ioValue shouldEqual false
      store.get("a").some shouldEqual RevisionedValue(2, "aa")
    }

    "return all entries" in {
      store.putIfAbsent("b", RevisionedValue(1, "b")).ioValue
      store.entries().ioValue shouldEqual Map(
        "b" -> RevisionedValue(1, "b"),
        "a" -> RevisionedValue(2, "aa")
      )
    }

    "return all values" in {
      store.values().ioValue shouldEqual Set(RevisionedValue(1, "b"), RevisionedValue(2, "aa"))
    }

    "return a matching (key, value)" in {
      store.find({ case (k, _) => k == "a" }).some shouldEqual ("a" -> RevisionedValue(2, "aa"))
    }

    "fail to return a matching (key, value)" in {
      store.find({ case (k, _) => k == "c" }).ioValue.isEmpty shouldEqual true
    }

    "return a matching value" in {
      store.findValue(_.value == "aa").some shouldEqual RevisionedValue(2, "aa")
    }

    "fail to return a matching value" in {
      store.findValue(_.value == "cc").ioValue.isEmpty shouldEqual true
    }

    "update values computing from current value" in {
      store.computeIfPresent("a", c => c.copy(c.rev + 1, c.value + "c")).ioValue shouldEqual
        Option(RevisionedValue(3, "aac"))
      store.get("a").some shouldEqual RevisionedValue(3, "aac")
    }

    "discard updates on computing value when new revision is not greater than current" in {
      store.computeIfPresent("a", c => c.copy(c.rev, c.value + "d")).ioValue
      store.get("a").some shouldEqual RevisionedValue(3, "aac")
    }

    "discard updates on computing value when key does not exist" in {
      store.computeIfPresent("c", c => c.copy(c.rev, c.value + "d")).ioValue shouldEqual None
      store.get("c").ioValue shouldEqual None
    }

    "return empty entries" in {
      val store = KeyValueStore.distributed[IO, String, RevisionedValue[String]]("empty", { case (_, rv) => rv.rev })
      store.entries().ioValue shouldEqual Map.empty[String, RevisionedValue[String]]
    }

  }

  override implicit def patienceConfig = PatienceConfig(6 seconds, 100 millis)
}

object KeyValueStoreSpec {
  final case class RevisionedValue[A](rev: Long, value: A)
}
