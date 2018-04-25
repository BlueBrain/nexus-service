package ch.epfl.bluebrain.nexus.service.http.routes

import java.io.File
import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import io.circe.Json
import io.circe.parser.parse

import scala.io.Source

/**
  * Routes that expose static resources provided on the classpath
  * @param resourcePath path to the folder in classpath to expose
  * @param prefix       prefix to prepend to the routes
  * @param baseUri      base URI to use in IDs, will replace {{base}} in all the resources
  */
class StaticResourceRoutes(resourcePath: String, prefix: String, baseUri: Uri) {

  private def contentOf(file: File): String =
    Source.fromFile(file).mkString

  private def contentOf(file: File, replacements: Map[String, String]): String =
    replacements.foldLeft(contentOf(file)) {
      case (value, (regex, replacement)) => value.replaceAll(regex, replacement)
    }

  private val baseReplacement: Map[String, String] = Map(quote("{{base}}") -> baseUri.toString)

  private def folderContents(folder: File): Map[String, Json] = {
    folder
      .listFiles()
      .toList
      .filter(f => f.isFile && f.getName.endsWith(".json"))
      .flatMap { file =>
        parse(contentOf(file, baseReplacement)).toOption.map { json =>
          file.getName.stripSuffix(".json") -> json
        }
      }
      .toMap
  }

  private lazy val resources: Map[String, Map[String, Json]] = {
    val resourceFolder = new File(getClass.getResource(resourcePath).getPath)
    if (resourceFolder.exists && resourceFolder.isDirectory) {
      resourceFolder
        .listFiles()
        .toList
        .filter(_.isDirectory)
        .map { folder =>
          folder.getName -> folderContents(folder)
        }
        .toMap
    } else {
      Map()
    }
  }

  def routes: Route =
    (get & pathPrefix(prefix)) {
      path(Segment / Segment) { (resourceType, resource) =>
        resources.get(resourceType).flatMap(_.get(resource)) match {
          case Some(json) => complete(json)
          case None       => complete(StatusCodes.NotFound)
        }
      }
    }

}
