package ch.epfl.bluebrain.nexus.service.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Rejection
import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.{Json, Printer}
import io.circe.parser._
import org.scalatest.{EitherValues, Inspectors, Matchers, WordSpecLike}

class RejectionHandlingSpec
    extends WordSpecLike
    with Matchers
    with Inspectors
    with ScalatestRouteTest
    with EitherValues {

  class Custom extends Rejection

  private implicit val printer: Printer = Printer.spaces2.copy(dropNullValues = true)

  "A default rejection handler" should {
    val handler =
      RejectionHandling { _: Custom =>
        StatusCodes.InternalServerError -> Json.obj("reason" -> Json.fromString("custom"))
      }.withFallback(RejectionHandling.notFound)

    "handle not found" in {
      val route = handleRejections(handler)(pathEnd(complete("ok")))
      Get("/a") ~> route ~> check {
        val expected =
          s"""{
             |  "@context": "https://bluebrain.github.io/nexus/contexts/error.json",
             |  "@type": "NotFound",
             |  "reason": "The requested resource could not be found."
             |}""".stripMargin
        status shouldEqual StatusCodes.NotFound
        responseAs[Json] shouldEqual parse(expected).right.value
      }
    }

    "handle missing query param" in {
      val route = handleRejections(handler)(parameter('rev.as[Long])(_ => complete("ok")))
      Get("/a") ~> route ~> check {
        val expected =
          s"""{
             |  "@context": "https://bluebrain.github.io/nexus/contexts/error.json",
             |  "@type": "MissingQueryParam",
             |  "reason": "Request is missing required query parameter 'rev'."
             |}""".stripMargin
        status shouldEqual StatusCodes.BadRequest
        responseAs[Json] shouldEqual parse(expected).right.value
      }
    }

    "handle custom" in {
      val route = handleRejections(handler)(reject(new Custom))
      Get("/a") ~> route ~> check {
        val expected =
          s"""{
             |  "reason": "custom"
             |}""".stripMargin
        status shouldEqual StatusCodes.InternalServerError
        responseAs[Json] shouldEqual parse(expected).right.value
      }
    }
  }

}
