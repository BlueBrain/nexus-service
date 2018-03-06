package ch.epfl.bluebrain.nexus.service.kamon.directives

import akka.http.scaladsl.model.{ContentTypes, HttpCharsets, MediaType}
import com.typesafe.config.Config

import scala.util.Try

/**
  * Configuration data type for payload tracing.
  *
  * @param maxContentLength  the maximum content length size for which payloads are included as tracing tags
  * @param payloadTagName    the name of the tag to be added to the collection of span tags
  * @param payloadMediaTypes the media types for which the payload should be traced
  */
final case class TracingConfig(maxContentLength: Long, payloadTagName: String, payloadMediaTypes: Set[MediaType])

object TracingConfig {

  def default: TracingConfig =
    TracingConfig(
      maxContentLength = 20L * 1024, // 20 KB
      payloadTagName = "payload",
      payloadMediaTypes = Set(ContentTypes.`application/json`.mediaType,
                              MediaType.applicationWithFixedCharset("ld+json", HttpCharsets.`UTF-8`, "jsonld"))
    )

  def fromConfig(config: Config): Either[String, TracingConfig] = {
    import scala.collection.JavaConverters._
    for {
      mcl <- Try(config.getLong("kamon.trace.payload.max-content-length")).toEither.left.map(_ =>
        "Illegal config value 'kamon.trace.payload.max-content-length'")
      ptn <- Try(config.getString("kamon.trace.payload.payload-tag-name")).toEither.left.map(_ =>
        "Illegal config value 'kamon.trace.payload.payload-tag-name'")
      pmt <- Try {
        val list           = config.getStringList("kamon.trace.payload.payload-media-types").asScala.toList
        val parsingResults = list.map(s => MediaType.parse(s))
        parsingResults.foldLeft(Set.empty[MediaType]) {
          case (_, Left(_))     => throw new IllegalArgumentException
          case (mts, Right(mt)) => mts + mt
        }
      }.toEither.left.map(_ => "Illegal config value 'kamon.trace.payload.payload-media-types'")
    } yield TracingConfig(mcl, ptn, pmt)
  }
}
