package ch.epfl.bluebrain.nexus.service.kamon.directives

import akka.http.scaladsl.model.{ContentTypes, HttpCharsets, MediaType}
import com.typesafe.config.Config

import scala.util.Try

/**
  * Configuration data type for request entity tracing.
  *
  * @param maxContentLength the maximum content length size for which request entities are included as tracing tags
  * @param tagName          the name of the tag to be added to the collection of span tags
  * @param mediaTypes       the media types for which the request entity should be traced
  */
final case class TracingConfig(maxContentLength: Long, tagName: String, mediaTypes: Set[MediaType])

object TracingConfig {

  def default: TracingConfig =
    TracingConfig(
      maxContentLength = 20L * 1024, // 20 KB
      tagName = "http.request.entity",
      mediaTypes = Set(ContentTypes.`application/json`.mediaType,
                       MediaType.applicationWithFixedCharset("ld+json", HttpCharsets.`UTF-8`, "jsonld"))
    )

  def fromConfig(config: Config): Either[String, TracingConfig] = {
    import scala.collection.JavaConverters._
    for {
      mcl <- Try(config.getLong("kamon.trace.request-entity.max-content-length")).toEither.left.map(_ =>
        "Illegal config value 'kamon.trace.request-entity.max-content-length'")
      ptn <- Try(config.getString("kamon.trace.request-entity.tag-name")).toEither.left.map(_ =>
        "Illegal config value 'kamon.trace.request-entity.tag-name'")
      pmt <- Try {
        val list           = config.getStringList("kamon.trace.request-entity.media-types").asScala.toList
        val parsingResults = list.map(s => MediaType.parse(s))
        parsingResults.foldLeft(Set.empty[MediaType]) {
          case (_, Left(_))     => throw new IllegalArgumentException
          case (mts, Right(mt)) => mts + mt
        }
      }.toEither.left.map(_ => "Illegal config value 'kamon.trace.request-entity.media-types'")
    } yield TracingConfig(mcl, ptn, pmt)
  }
}
