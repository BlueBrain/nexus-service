package ch.epfl.bluebrain.nexus.service.http

import akka.http.scaladsl.server.Directives.{complete, get}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.service.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.service.http.JsonOps._
import ch.epfl.bluebrain.nexus.service.http.JsonOpsSpec._
import ch.epfl.bluebrain.nexus.commons.test.Resources
import io.circe.Json
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.collection.mutable.LinkedHashSet

class JsonOpsSpec extends WordSpecLike with Matchers with Resources with Inspectors with ScalatestRouteTest {

  "A JsonOps" when {
    implicit val config: Configuration = Configuration.default.withDiscriminator("@type")
    implicit val context: ContextUri   = ContextUri("https://bbp-nexus.epfl.ch/dev/v0/contexts/bbp/core/context/v0.1.0")

    "dealing with KG data" should {
      val list = List(
        (jsonContentOf("/kg_json/activity_schema.json")   -> jsonContentOf("/kg_json/activity_schema_ordered.json")),
        (jsonContentOf("/kg_json/activity_instance.json") -> jsonContentOf("/kg_json/activity_instance_ordered.json")),
        (jsonContentOf("/kg_json/activity_instance_att.json") -> jsonContentOf(
          "/kg_json/activity_instance_att_ordered.json"))
      )
      implicit val _ = OrderedKeys(
        List(
          "@context",
          "@id",
          "@type",
          "self",
          "",
          "nxv:rev",
          "nxv:originalFileName",
          "nxv:contentType",
          "nxv:size",
          "nxv:unit",
          "nxv:digest",
          "nxv:alg",
          "nxv:value",
          "nxv:published",
          "nxv:deprecated",
          "links"
        ))

      "order jsonLD input" in {
        forAll(list) {
          case (unordered, expected) =>
            unordered.sortKeys.spaces2 shouldEqual expected.spaces2
        }
      }

      "generated jsonLD HTTP" in {
        val route = get {
          complete(
            KgResponse(
              "cValue",
              true,
              1L,
              "aValue",
              Map("self" -> "http://localhost/link1", "schema" -> "http://localhost/link2"),
              "https://bbp-nexus.epfl.ch/dev/v0/schemas/bbp/core/schema/v0.1.0",
              false,
              "owl:Ontology"
            ))
        }
        Get("/") ~> route ~> check {
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          entityAs[Json].spaces2 shouldEqual jsonContentOf("/kg_json/kg_fake_schema.json").spaces2
        }
      }
    }

    "dealing with IAM data" should {
      val list = List(
        (jsonContentOf("/iam_json/acls.json") -> jsonContentOf("/iam_json/acls_ordered.json")),
        (jsonContentOf("/iam_json/user.json") -> jsonContentOf("/iam_json/user_ordered.json"))
      )

      implicit val _ = OrderedKeys(
        List(
          "@context",
          "@id",
          "@type",
          "identity",
          "permissions",
          "realm",
          ""
        ))

      "order jsonLD input" in {
        forAll(list) {
          case (unordered, expected) =>
            unordered.sortKeys.spaces2 shouldEqual expected.spaces2
        }
      }

      "generated jsonLD HTTP" in {
        val route = get {
          complete(
            AuthenticatedUser(LinkedHashSet(
              GroupRef("bbp-user-one", "BBP", "https://nexus.example.com/v0/realms/BBP/groups/bbp-user-one"),
              GroupRef("bbp-svc-two", "BBP", "https://nexus.example.com/v0/realms/BBP/groups/bbp-svc-two"),
              Anonymous("https://nexus.example.com/v0/anonymous"),
              UserRef("f:434t3-134e-4444-aa74-bdf00f48dfce:some",
                      "BBP",
                      "https://nexus.example.com/v0/realms/BBP/users/f:434t3-134e-4444-aa74-bdf00f48dfce:some"),
              AuthenticatedRef(Some("BBP"), "https://nexus.example.com/v0/realms/BBP/authenticated")
            )): User)
        }
        Get("/") ~> route ~> check {
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          entityAs[Json].spaces2 shouldEqual jsonContentOf("/iam_json/user_ordered.json").spaces2

        }
      }
    }

    "manipulating json" should {
      "remove keys object" in {
        val obj = Json.obj("one" -> Json.obj("two" -> Json.fromString("something")), "two" -> Json.fromString("abc"))
        obj.removeKeys("two") shouldEqual Json.obj("one" -> Json.obj("two" -> Json.fromString("something")))
        obj.removeKeys("three") shouldEqual obj
      }
      "remove keys array" in {
        val obj      = Json.obj("one" -> Json.obj("two" -> Json.fromString("something")), "two" -> Json.fromString("abc"))
        val objNoKey = Json.obj("one" -> Json.obj("two" -> Json.fromString("something")))
        val arrObj   = Json.arr(obj, obj, Json.obj("three" -> Json.fromString("something")))
        arrObj.removeKeys("two") shouldEqual Json.arr(objNoKey,
                                                      objNoKey,
                                                      Json.obj("three" -> Json.fromString("something")))
      }
    }

    "injecting context" should {
      val contextString = Json.fromString(context.toString)

      val mapping = List(
        Json.obj("@id"        -> Json.fromString("foo-id"), "nxv:rev" -> Json.fromLong(1)) ->
          Json.obj("@context" -> contextString, "@id" -> Json.fromString("foo-id"), "nxv:rev" -> Json.fromLong(1)),
        Json.obj("@context"   -> Json.fromString("http://foo.domain/some/context"),
                 "@id"        -> Json.fromString("foo-id"),
                 "nxv:rev"    -> Json.fromLong(1)) ->
          Json.obj(
            "@context" -> Json.arr(Json.fromString("http://foo.domain/some/context"), contextString),
            "@id"      -> Json.fromString("foo-id"),
            "nxv:rev"  -> Json.fromLong(1)
          ),
        Json.obj(
          "@context" -> Json.arr(Json.fromString("http://foo.domain/some/context"),
                                 Json.fromString("http://bar.domain/another/context")),
          "@id"     -> Json.fromString("foo-id"),
          "nxv:rev" -> Json.fromLong(1)
        ) ->
          Json.obj(
            "@context" -> Json.arr(Json.fromString("http://foo.domain/some/context"),
                                   Json.fromString("http://bar.domain/another/context"),
                                   contextString),
            "@id"     -> Json.fromString("foo-id"),
            "nxv:rev" -> Json.fromLong(1)
          ),
        Json.obj(
          "@context" -> Json.obj("foo" -> Json.fromString("http://foo.domain/some/context"),
                                 "bar" -> Json.fromString("http://bar.domain/another/context")),
          "@id"     -> Json.fromString("foo-id"),
          "nxv:rev" -> Json.fromLong(1)
        ) ->
          Json.obj(
            "@context" -> Json.arr(Json.obj("foo" -> Json.fromString("http://foo.domain/some/context"),
                                            "bar" -> Json.fromString("http://bar.domain/another/context")),
                                   contextString),
            "@id"     -> Json.fromString("foo-id"),
            "nxv:rev" -> Json.fromLong(1)
          )
      )

      "properly add or merge context into JSON payload" in {
        forAll(mapping) {
          case (in, out) =>
            in.addContext(context) shouldEqual out
        }
      }

      "be idempotent" in {
        forAll(mapping) {
          case (in, _) =>
            in.addContext(context) shouldEqual in.addContext(context).addContext(context)
        }
      }
    }
  }
}

object JsonOpsSpec {
  final case class KgResponse(c: String,
                              `nxv:published`: Boolean,
                              `nxv:rev`: Long,
                              a: String,
                              links: Map[String, String],
                              `@id`: String,
                              `nxv:deprecated`: Boolean,
                              `@type`: String)

  sealed trait User extends Product with Serializable {
    def identities: LinkedHashSet[Identity]
  }
  final case class AuthenticatedUser(identities: LinkedHashSet[Identity]) extends User
  sealed trait Identity extends Product with Serializable {
    def `@id`: String
  }
  final case class GroupRef(group: String, realm: String, `@id`: String)  extends Identity
  final case class UserRef(sub: String, realm: String, `@id`: String)     extends Identity
  final case class AuthenticatedRef(realm: Option[String], `@id`: String) extends Identity
  final case class Anonymous(`@id`: String)                               extends Identity
}
