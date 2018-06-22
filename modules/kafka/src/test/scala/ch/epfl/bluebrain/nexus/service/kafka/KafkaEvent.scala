package ch.epfl.bluebrain.nexus.service.kafka

import ch.epfl.bluebrain.nexus.service.kafka.key.Key
import io.circe._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

case class KafkaEvent(`@context`: String, _id: String, _rev: Long)

object KafkaEvent {

  private implicit val config: Configuration = Configuration.default.withDiscriminator("@type")

  implicit val eventDecoder: Decoder[KafkaEvent] = deriveDecoder[KafkaEvent]

  implicit val eventEncoder: Encoder[KafkaEvent] = deriveEncoder[KafkaEvent]

  implicit val key: Key[KafkaEvent] = Key.key(_._id)
}
