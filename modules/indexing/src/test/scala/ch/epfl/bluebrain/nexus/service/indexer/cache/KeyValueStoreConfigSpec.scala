package ch.epfl.bluebrain.nexus.service.indexer.cache

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.RetryStrategyConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, OptionValues, WordSpecLike}
import pureconfig.loadConfigOrThrow

import scala.concurrent.duration._

class KeyValueStoreConfigSpec
    extends TestKit(ActorSystem("KeyValueStoreConfigSpec"))
    with WordSpecLike
    with Matchers
    with OptionValues {

  val config = KeyValueStoreConfig(10 seconds, 10 seconds, RetryStrategyConfig("exponential", 100 millis, 7, 2))

  "KeyValueStoreConfig" should {

    "read from config file" in {
      val readConfig = ConfigFactory.parseFile(new File(getClass.getResource("/example-store.conf").toURI))
      loadConfigOrThrow[KeyValueStoreConfig](readConfig, "key-value-store") shouldEqual config
    }
  }
}
