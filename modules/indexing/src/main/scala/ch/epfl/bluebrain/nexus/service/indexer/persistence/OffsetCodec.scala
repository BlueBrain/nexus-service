package ch.epfl.bluebrain.nexus.service.indexer.persistence

import java.util.UUID

import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import io.circe._
import shapeless.Typeable

/**
  * Collection of implicitly available encoders and decoders for PersistenceQuery's [[akka.persistence.query.Offset]].
  */
trait OffsetCodec {

  final implicit val noOffsetTypeable: Typeable[NoOffset.type] =
    new Typeable[NoOffset.type] {
      override def cast(t: Any): Option[NoOffset.type] =
        if (NoOffset == t) Some(NoOffset) else None
      override def describe: String = "NoOffset"
    }

  final implicit val sequenceEncoder: Encoder[Sequence] = Encoder.instance { seq =>
    Json.obj("value" -> Json.fromLong(seq.value))
  }

  final implicit val sequenceDecoder: Decoder[Sequence] = Decoder.instance { cursor =>
    cursor.get[Long]("value").map(value => Sequence(value))
  }

  final implicit val timeBasedUUIDEncoder: Encoder[TimeBasedUUID] = Encoder.instance { uuid =>
    Json.obj("value" -> Encoder.encodeUUID(uuid.value))
  }

  final implicit val timeBasedUUIDDecoder: Decoder[TimeBasedUUID] = Decoder.instance { cursor =>
    cursor.get[UUID]("value").map(uuid => TimeBasedUUID(uuid))
  }

  final implicit val noOffsetEncoder: Encoder[NoOffset.type] = Encoder.instance(_ => Json.obj())

  final implicit val noOffsetDecoder: Decoder[NoOffset.type] = Decoder.instance { cursor =>
    cursor.as[JsonObject].map(_ => NoOffset)
  }

  final implicit val offsetEncoder: Encoder[Offset] = Encoder.instance {
    case o: Sequence      => encodeDiscriminated(o)
    case o: TimeBasedUUID => encodeDiscriminated(o)
    case o: NoOffset.type => encodeDiscriminated(o)
  }

  final implicit val offsetDecoder: Decoder[Offset] = Decoder.instance { cursor =>
    val sequence      = Typeable[Sequence].describe
    val timeBasedUUID = Typeable[TimeBasedUUID].describe
    val noOffset      = Typeable[NoOffset.type].describe
    cursor.get[String]("type").flatMap {
      case `sequence`      => cursor.as[Sequence]
      case `timeBasedUUID` => cursor.as[TimeBasedUUID]
      case `noOffset`      => cursor.as[NoOffset.type]
      // $COVERAGE-OFF$
      case other => Left(DecodingFailure(s"Unknown discriminator value '$other'", cursor.history))
      // $COVERAGE-ON$
    }
  }

  private def encodeDiscriminated[A: Encoder: Typeable](a: A) =
    Encoder[A].apply(a).deepMerge(Json.obj("type" -> Json.fromString(Typeable[A].describe)))
}

object OffsetCodec extends OffsetCodec
