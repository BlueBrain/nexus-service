package ch.epfl.bluebrain.nexus.service.http.routes

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.test.Resources
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import java.util.regex.Pattern.quote

import io.circe.parser.parse

class StaticResourceRoutesSpec
    extends WordSpecLike
    with Matchers
    with Inspectors
    with ScalatestRouteTest
    with Resources {

  val baseUri = "http://nexus.example.com/v1"

  val staticRoutes = new StaticResourceRoutes(
    Map(
      "/contexts/context1" -> "/static-routes-test/contexts/context1.json",
      "/contexts/context2" -> "/static-routes-test/contexts/context2.json",
      "/schemas/schema1"   -> "/static-routes-test/schemas/schema1.json",
      "/schemas/schema2"   -> "/static-routes-test/schemas/schema2.json"
    ),
    "test",
    baseUri
  ).routes

  val baseReplacement = Map(
    quote("{{base}}") -> baseUri
  )
  val files = Map(
    "/v1/test/contexts/context1" -> jsonContentOf("/static-routes-test/contexts/context1.json", baseReplacement),
    "/v1/test/contexts/context2" -> jsonContentOf("/static-routes-test/contexts/context2.json", baseReplacement),
    "/v1/test/schemas/schema1"   -> jsonContentOf("/static-routes-test/schemas/schema1.json", baseReplacement),
    "/v1/test/schemas/schema2"   -> jsonContentOf("/static-routes-test/schemas/schema2.json", baseReplacement)
  )

  "A StaticResourceRoutes" should {

    "return static resources" in {
      forAll(files.toList) {
        case (path, json) =>
          Get(path) ~> staticRoutes ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            parse(responseAs[String]).toOption.get shouldEqual json
          }
      }

    }

    "return 404 when resource doesn't exist" in {
      Get(s"/v1/test/schemas/${UUID.randomUUID().toString}") ~> staticRoutes ~> check {
        rejections shouldEqual Seq()
      }
    }
  }

}
