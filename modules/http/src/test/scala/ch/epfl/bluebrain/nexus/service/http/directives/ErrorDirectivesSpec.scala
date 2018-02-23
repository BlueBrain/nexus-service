package ch.epfl.bluebrain.nexus.service.http.directives

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCodes, Uri}
import akka.testkit.TestKit
import akka.util.ByteString
import ch.epfl.bluebrain.nexus.commons.http.{ContextUri, RdfMediaTypes}
import ch.epfl.bluebrain.nexus.service.http.directives.ErrorDirectives._
import ch.epfl.bluebrain.nexus.service.http.directives.ErrorDirectivesSpec.CustomError
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class ErrorDirectivesSpec
    extends TestKit(ActorSystem("ErrorDirectivesSpec"))
    with WordSpecLike
    with Matchers
    with ScalaFutures {

  override implicit val patienceConfig = PatienceConfig(3 seconds, 100 millis)

  "A ErrorDirectives" should {
    import system.dispatcher
    implicit val statusFromJson: StatusFrom[CustomError] = StatusFrom((err: CustomError) => StatusCodes.NotFound)
    implicit val contextUri: ContextUri                  = ContextUri(Uri("http://localhost.com/error/"))

    "marshall error JSON-LD" in {
      val error      = CustomError("some error")
      val jsonString = s"""{"message":"${error.message}","@context":"${contextUri.context}"}"""
      Marshal(error).to[HttpResponse].futureValue shouldEqual HttpResponse(
        status = StatusCodes.NotFound,
        entity = HttpEntity.Strict(RdfMediaTypes.`application/ld+json`, ByteString(jsonString, "UTF-8")))
    }
  }

}

object ErrorDirectivesSpec {
  final case class CustomError(message: String)
}
