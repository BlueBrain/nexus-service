package ch.epfl.bluebrain.nexus.service.http.routes

import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.service.http.directives.PrefixDirectives
import io.circe.Json
import io.circe.parser.parse

import scala.io.Source

/**
  * Routes that expose static resources provided on the classpath
  * @param resourcePaths [[Map]] containing mapping between path at which resource will be available(starting with `/`)
  *                     and the path where the resource can be found on the classpath.
  * @param prefix       prefix to prepend to the routes
  * @param baseUri      base URI to use in IDs, will replace {{base}} in all the resources
  */
class StaticResourceRoutes(resourcePaths: Map[String, String], prefix: String, baseUri: Uri) extends PrefixDirectives {

  private def contentOf(file: String): String =
    Source.fromInputStream(getClass.getResourceAsStream(file)).mkString

  private def contentOf(file: String, replacements: Map[String, String]): String =
    replacements.foldLeft(contentOf(file)) {
      case (value, (regex, replacement)) => value.replaceAll(regex, replacement)
    }

  private val baseReplacement: Map[String, String] = Map(quote("{{base}}") -> baseUri.toString)

  private lazy val resources: Map[String, Json] =
    resourcePaths
      .mapValues { resource =>
        parse(contentOf(resource, baseReplacement)).toOption
      }
      .flatMap {
        case (key, value) =>
          value match {
            case Some(v) => Some((key, v))
            case None    => None
          }
      }

  def routes: Route =
    uriPrefix(baseUri) {
      (get & pathPrefix(prefix)) {
        extractUnmatchedPath { resourcePath =>
          resources.get(resourcePath.toString) match {
            case Some(json) => complete(json)
            case None       => reject
          }
        }
      }
    }

}
