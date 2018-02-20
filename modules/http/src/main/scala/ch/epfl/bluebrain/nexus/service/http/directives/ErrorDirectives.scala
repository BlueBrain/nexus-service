package ch.epfl.bluebrain.nexus.service.http.directives

import akka.http.scaladsl.marshalling.{Marshaller, ToResponseMarshaller}
import akka.http.scaladsl.model._
import ch.epfl.bluebrain.nexus.service.http.{ContextUri, RdfMediaTypes}
import ch.epfl.bluebrain.nexus.service.http.JsonOps._
import io.circe.Encoder

/**
  * Directive to marshall StatusFrom instances into HTTP responses.
  */
object ErrorDirectives {

  /**
    * Implicitly derives a generic [[akka.http.scaladsl.marshalling.ToResponseMarshaller]] based on
    * implicitly available [[StatusFrom]] and [[io.circe.Encoder]] instances, and a [[ContextUri]], that provides
    * a JSON-LD response from an entity of type ''A''.
    *
    * @tparam A the generic type for which the marshaller is derived
    * @param statusFrom the StatusFrom instance mapping the entity to an HTTP status
    * @param encoder the Circe encoder instance to convert the entity into JSON
    * @param context the context URI to be injected into the JSON-LD response body
    * @return a ''ToResponseMarshaller'' that will generate an appropriate JSON-LD response
    */
  final implicit def jsonLdMarshallerFromStatusAndEncoder[A](
      implicit
      statusFrom: StatusFrom[A],
      encoder: Encoder[A],
      context: ContextUri
  ): ToResponseMarshaller[A] =
    Marshaller.withFixedContentType(RdfMediaTypes.`application/ld+json`) { value =>
      HttpResponse(status = statusFrom(value),
                   entity = HttpEntity(RdfMediaTypes.`application/ld+json`,
                                       encoder.mapJson(_.addContext(context)).apply(value).noSpaces))
    }
}
