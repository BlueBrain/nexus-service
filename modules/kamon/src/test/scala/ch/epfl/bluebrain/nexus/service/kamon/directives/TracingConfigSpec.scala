package ch.epfl.bluebrain.nexus.service.kamon.directives

import com.typesafe.config.ConfigFactory
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class TracingConfigSpec extends WordSpecLike with Matchers with EitherValues {

  "A TracingConfig" should {
    "present appropriate defaults" in {
      val config = ConfigFactory.parseString(
        """
          |kamon.trace {
          |  payload {
          |      max-content-length = 20480 // 20 KB
          |      payload-tag-name = "payload"
          |      payload-media-types = [
          |        "application/json",
          |        "application/ld+json"
          |      ]
          |    }
          |}
        """.stripMargin)
      TracingConfig.default shouldEqual TracingConfig.fromConfig(config).right.value
    }
    "be constructed correctly from config" in {
      val config = ConfigFactory.parseString(
        """
          |kamon.trace {
          |  payload {
          |      max-content-length = 20480 // 20 KB
          |      payload-tag-name = "payload"
          |      payload-media-types = [
          |        "application/json",
          |        "application/ld+json"
          |      ]
          |    }
          |}
        """.stripMargin)
      TracingConfig.fromConfig(config).right.value
    }
    "fail to construct from config" when {
      "using an illegal 'max-content-length'" in {
        val config = ConfigFactory.parseString(
          """
            |kamon.trace {
            |  payload {
            |      max-content-length = "asd"
            |      payload-tag-name = "payload"
            |      payload-media-types = [
            |        "application/json",
            |        "application/ld+json"
            |      ]
            |    }
            |}
          """.stripMargin)
        TracingConfig.fromConfig(config).left.value
      }
      "using an illegal 'payload-tag-name'" in {
        val config = ConfigFactory.parseString(
          """
            |kamon.trace {
            |  payload {
            |      max-content-length = 3
            |      payload-tag-name = []
            |      payload-media-types = [
            |        "application/json",
            |        "application/ld+json"
            |      ]
            |    }
            |}
          """.stripMargin)
        TracingConfig.fromConfig(config).left.value
      }
      "using an illegal 'payload-media-types'" in {
        val config = ConfigFactory.parseString(
          """
            |kamon.trace {
            |  payload {
            |      max-content-length = 3
            |      payload-tag-name = "payload"
            |      payload-media-types = [
            |        "application/json 123",
            |        "application/ld+json"
            |      ]
            |    }
            |}
          """.stripMargin)
        TracingConfig.fromConfig(config).left.value
      }
    }
  }

}
