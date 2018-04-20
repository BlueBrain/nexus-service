package ch.epfl.bluebrain.nexus.service.http

import akka.http.scaladsl.model.Uri

trait UriOps {

  /**
    * Syntax sugar that allows appending a [[Path]] to a [[Uri]] without duplicating trailing slashes.
    * Example:
    * Uri("http://localhost/a/b/").append(Path("/c"))
    * res: Uri("http://localhost/a/b/c")
    */
  implicit class UriSyntax(uri: Uri) {
    def append(path: Path): Uri =
      uri.copy(path = (uri.path: Path) ++ path)
  }
}

object UriOps extends UriOps
