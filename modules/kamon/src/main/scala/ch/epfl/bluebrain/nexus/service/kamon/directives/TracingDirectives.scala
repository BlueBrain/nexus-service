package ch.epfl.bluebrain.nexus.service.kamon.directives

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentType, HttpCharsets, HttpEntity}
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.Directives.extractRequestEntity
import akka.util.ByteString
import kamon.akka.http.KamonTraceDirectives.operationName

/**
  * Custom Kamon tracing directives that are preconfigured for the typical service use cases.
  *
  * @param config the tracing configuration
  */
class TracingDirectives(config: TracingConfig = TracingConfig.default) {

  private def validTracingCandidate(entity: HttpEntity.Strict): Boolean =
    config.mediaTypes.contains(entity.contentType.mediaType) && entity.contentLength < config.maxContentLength

  private def tagEntry(ct: ContentType, data: ByteString): (String, String) =
    config.tagName -> data.decodeString(ct.charsetOption.getOrElse(HttpCharsets.`UTF-8`).value)

  /**
    * Configures the current span with the arguments.
    *
    * @param name                 the operation name of the span
    * @param tags                 the collection of tags to add to the span
    * @param includeRequestEntity whether to attempt include the request entity as a tag
    */
  def trace(name: String, tags: Map[String, String] = Map.empty, includeRequestEntity: Boolean = true): Directive0 = {
    if (!includeRequestEntity) operationName(name, tags)
    else
      extractRequestEntity.flatMap {
        case entity @ HttpEntity.Strict(ct, data) if validTracingCandidate(entity) =>
          operationName(name, tags + tagEntry(ct, data))
        case _ => operationName(name, tags)
      }
  }
}

object TracingDirectives {

  /**
    * Constructs a new [[TracingDirectives]] instance using the argument config.
    *
    * @param config the tracing configuration
    */
  final def apply(config: TracingConfig): TracingDirectives =
    new TracingDirectives(config)

  /**
    * Constructs a new [[TracingDirectives]] instance using the underlying [[ActorSystem]] configuration.
    *
    * @param as the underlying actor system
    */
  // $COVERAGE-OFF$
  final def apply()(implicit as: ActorSystem): TracingDirectives = {
    val config = TracingConfig
      .fromConfig(as.settings.config)
      .left
      .map(str => new IllegalArgumentException(str))
      .fold[TracingConfig](e => throw e, identity)
    apply(config)
  }

  // $COVERAGE-ON$
}
