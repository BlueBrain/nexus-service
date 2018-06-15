package ch.epfl.bluebrain.nexus.service.queue

import cats.Show
import io.circe._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

case class KafkaEvent(`@context`: String, _id: String, _rev: Long)

object KafkaEvent {

  private implicit val config: Configuration = Configuration.default.withDiscriminator("@type")

  implicit val eventDecoder: Decoder[KafkaEvent] = deriveDecoder[KafkaEvent]

  implicit val eventEncoder: Encoder[KafkaEvent] = deriveEncoder[KafkaEvent]

  implicit val show: Show[KafkaEvent] = Show.show(_._id)
}
