package ch.epfl.bluebrain.nexus.service.kamon.directives

import com.typesafe.config.ConfigFactory
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class TracingConfigSpec extends WordSpecLike with Matchers with EitherValues {

  "A TracingConfig" should {
    "present appropriate defaults" in {
      val config = ConfigFactory.parseString("""
          |kamon.trace {
          |  request-entity {
          |      max-content-length = 20480 // 20 KB
          |      tag-name = "http.request.entity"
          |      media-types = [
          |        "application/json",
          |        "application/ld+json"
          |      ]
          |    }
          |}
        """.stripMargin)
      TracingConfig.default shouldEqual TracingConfig.fromConfig(config).right.value
    }
    "be constructed correctly from config" in {
      val config = ConfigFactory.parseString("""
          |kamon.trace {
          |  request-entity {
          |      max-content-length = 20480 // 20 KB
          |      tag-name = "http.request.entity"
          |      media-types = [
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
        val config = ConfigFactory.parseString("""
            |kamon.trace {
            |  request-entity {
            |      max-content-length = "asd"
            |      tag-name = "http.request.entity"
            |      media-types = [
            |        "application/json",
            |        "application/ld+json"
            |      ]
            |    }
            |}
          """.stripMargin)
        TracingConfig.fromConfig(config).left.value
      }
      "using an illegal 'tag-name'" in {
        val config = ConfigFactory.parseString("""
            |kamon.trace {
            |  request-entity {
            |      max-content-length = 3
            |      tag-name = []
            |      media-types = [
            |        "application/json",
            |        "application/ld+json"
            |      ]
            |    }
            |}
          """.stripMargin)
        TracingConfig.fromConfig(config).left.value
      }
      "using an illegal 'media-types'" in {
        val config = ConfigFactory.parseString("""
            |kamon.trace {
            |  request-entity {
            |      max-content-length = 3
            |      tag-name = "http.request.entity"
            |      media-types = [
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
