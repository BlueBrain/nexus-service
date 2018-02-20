package ch.epfl.bluebrain.nexus.service.http

import akka.http.scaladsl.model.HttpResponse

/**
  * Error type representing an unexpected unsuccessful http response.  Its entity bytes are discarded.
  *
  * @param response the underlying unexpected http response
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
final case class UnexpectedUnsuccessfulHttpResponse(response: HttpResponse) extends Exception {
  override def fillInStackTrace(): UnexpectedUnsuccessfulHttpResponse = this
  // $COVERAGE-OFF$
  override val getMessage: String = "Received an unexpected http response while communicating with an external service"
  // $COVERAGE-ON$
}
