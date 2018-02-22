package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetCodec._
import com.datastax.driver.core.utils.UUIDs
import io.circe.parser._
import io.circe.{Encoder, Json}
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import shapeless.Typeable

class OffsetCodecSpec extends WordSpecLike with Matchers with Inspectors {

  "An OffsetCodec" should {
    val uuid = UUIDs.timeBased()
    val mapping = Map(
      Sequence(14L)       -> """{"type":"Sequence","value":14}""",
      TimeBasedUUID(uuid) -> s"""{"type":"TimeBasedUUID","value":"$uuid"}""",
      NoOffset            -> """{"type":"NoOffset"}"""
    )

    "properly encode offset values" in {
      forAll(mapping.toList) {
        case (off, repr) =>
          Encoder[Offset].apply(off).noSpaces shouldEqual repr
      }
    }

    "properly decode offset values" in {
      forAll(mapping.toList) {
        case (off, repr) =>
          decode[Offset](repr) shouldEqual Right(off)
      }
    }
  }

  "A Typeable[NoOffset] instance" should {

    "cast a noOffset value" in {
      Typeable[NoOffset.type].cast(NoOffset) shouldEqual Some(NoOffset)
    }

    "not cast an arbitrary value" in {
      Typeable[NoOffset.type].cast(Json.obj()) shouldEqual None
    }

    "describe NoOffset by its object name" in {
      Typeable[NoOffset.type].describe shouldEqual "NoOffset"
    }
  }
}
