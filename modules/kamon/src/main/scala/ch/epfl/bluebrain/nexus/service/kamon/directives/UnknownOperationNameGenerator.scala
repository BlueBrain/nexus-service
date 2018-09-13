package ch.epfl.bluebrain.nexus.service.kamon.directives

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.Host
import kamon.akka.http.AkkaHttp

/**
  * An operation name generator that sets the value of the operation to ''unknown'' for all names. This needs to be
  * used in conjunction with the tracing directives that automatically configure the operation name for matched routes.
  *
  * Kamon automatically sets an operation name as part of the instrumentation based on the specified name generator
  * which can cause a high cardinality of ''span_processing_time_*'' metrics.
  */
// $COVERAGE-OFF$
class UnknownOperationNameGenerator extends AkkaHttp.OperationNameGenerator {

  override def clientOperationName(request: HttpRequest): String = {
    val uriAddress = request.uri.authority.host.address
    if (uriAddress.isEmpty) hostFromHeaders(request).getOrElse("unknown-host") else uriAddress
  }

  override def serverOperationName(request: HttpRequest): String =
    "unknown"

  private def hostFromHeaders(request: HttpRequest): Option[String] =
    request.header[Host].map(_.host.toString())
}
// $COVERAGE-ON$
