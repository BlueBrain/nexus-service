package ch.epfl.bluebrain.nexus.service.http

import akka.http.scaladsl.model.{HttpCharsets, MediaType}

/**
  * Collection of media types specific to RDF.
  */
object RdfMediaTypes {
  final val `text/turtle`: MediaType.WithOpenCharset = MediaType.applicationWithOpenCharset("turtle", "ttl")

  final val `application/rdf+xml`: MediaType.WithOpenCharset = MediaType.applicationWithOpenCharset("rdf+xml", "rdf")

  final val `application/ntriples`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("ntriples", HttpCharsets.`UTF-8`, "nt")

  final val `application/ld+json`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("ld+json", HttpCharsets.`UTF-8`, "jsonld")

  final val `application/sparql-results+json`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("sparql-results+json", HttpCharsets.`UTF-8`, "json")
}
