package ch.epfl.bluebrain.nexus.service.http.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMessage.DiscardedEntity
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, StatusCode}
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.stream.Materializer
import akka.util.ByteString
import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.service.http.UnexpectedUnsuccessfulHttpResponse
import journal.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
  * Contract definition for an HTTP client based on the akka http model.
  *
  * @tparam F the monadic effect type
  * @tparam A the unmarshalled return type of a request
  */
trait HttpClient[F[_], A] {

  /**
    * Execute the argument request and unmarshal the response into an ''A''.
    *
    * @param req the request to execute
    * @return an unmarshalled ''A'' value in the ''F[_]'' context
    */
  def apply(req: HttpRequest): F[A]

  /**
    * Discard the response bytes of the entity, if any.
    *
    * @param entity the entity that needs to be discarded
    * @return a discarded entity
    */
  def discardBytes(entity: HttpEntity): F[DiscardedEntity]

  /**
    * Attempt to transform the entity bytes (if any) into an UTF-8 string representation.  If the entity has no bytes
    * an empty string will be returned instead.
    *
    * @param entity the entity to transform into a string representation
    * @return the entity bytes (if any) into an UTF-8 string representation
    */
  def toString(entity: HttpEntity): F[String]

}

object HttpClient {

  /**
    * Type alias for [[HttpClient]] that has the unmarshalled return type
    * the [[akka.http.scaladsl.model.HttpResponse]] itself.
    *
    * @tparam F the monadic effect type
    */
  type UntypedHttpClient[F[_]] = HttpClient[F, HttpResponse]

  /**
    * Interface syntax to expose new functionality into [[F[HttpResponse]]] type
    *
    * @param resp the [[HttpResponse]] wrapped in ''F[_]''
    */
  implicit class HttpResponseSyntax[F[_]](resp: F[HttpResponse])(implicit F: MonadError[F, Throwable],
                                                                 cl: UntypedHttpClient[F]) {

    /**
      * Discards the response's bytes of the response's status matches some of ''expectedCodes''
      * or triggers ''onFailure'' otherwise
      *
      * @param expectedCodes the codes to verify against the response code
      * @param onFailure     the function to run when the response code does not match ''expectedCodes''
      */
    def discardOnCodesOr(expectedCodes: Set[StatusCode])(onFailure: => (HttpResponse) => F[Unit]): F[Unit] = {
      resp.flatMap { r =>
        if (expectedCodes.contains(r.status)) cl.discardBytes(r.entity).map(_ => ())
        else onFailure(r)
      }
    }
  }

  /**
    * Constructs an [[HttpClient.UntypedHttpClient]] instance using an
    * underlying akka http client.
    *
    * @param as an implicit actor system
    * @param mt an implicit materializer
    * @return an untyped http client based on akka http transport
    */
  // $COVERAGE-OFF$
  final def akkaHttpClient(implicit as: ActorSystem, mt: Materializer): UntypedHttpClient[Future] =
    new HttpClient[Future, HttpResponse] {
      import as.dispatcher

      override def apply(req: HttpRequest): Future[HttpResponse] =
        Http().singleRequest(req)

      override def discardBytes(entity: HttpEntity): Future[DiscardedEntity] =
        Future.successful(entity.discardBytes())

      override def toString(entity: HttpEntity): Future[String] =
        entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
    }

  /**
    * Constructs a typed ''HttpClient[Future, A]'' from an ''UntypedHttpClient[Future]'' by attempting to unmarshal the
    * response entity into the specific type ''A'' using an implicit ''FromEntityUnmarshaller[A]''.
    *
    * Delegates all calls to the underlying untyped http client.
    *
    * If the response status is not successful, the entity bytes will be discarded instead.
    *
    * @param ec an implicit execution context
    * @param mt an implicit materializer
    * @param cl an implicit untyped http client
    * @param um an implicit ''FromEntityUnmarshaller[A]''
    * @tparam A the specific type to which the response entity should be unmarshalled into
    */
  final implicit def withAkkaUnmarshaller[A: ClassTag](implicit
                                                       ec: ExecutionContext,
                                                       mt: Materializer,
                                                       cl: UntypedHttpClient[Future],
                                                       um: FromEntityUnmarshaller[A]): HttpClient[Future, A] =
    new HttpClient[Future, A] {

      private val log = Logger(s"TypedHttpClient[${implicitly[ClassTag[A]]}]")

      override def apply(req: HttpRequest): Future[A] =
        cl(req).flatMap { resp =>
          if (resp.status.isSuccess()) um(resp.entity)
          else {
            log.error(s"Unsuccessful HTTP response for '${req.uri}', status: '${resp.status}', discarding bytes")
            discardBytes(resp.entity).flatMap { _ =>
              log.debug(s"Discarded response bytes for request '${req.uri}'")
              Future.failed(UnexpectedUnsuccessfulHttpResponse(resp))
            }
          }
        }

      override def discardBytes(entity: HttpEntity): Future[DiscardedEntity] =
        cl.discardBytes(entity)

      override def toString(entity: HttpEntity): Future[String] =
        cl.toString(entity)
    }
  // $COVERAGE-ON$
}
