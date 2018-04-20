package ch.epfl.bluebrain.nexus.service.http

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.service.http.Path._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.service.http.UriOps._

class UriOpsSpec extends WordSpecLike with Matchers with Inspectors {
  "A UriOps" should {
    val list = List(
      (Uri("http://localhost/a"), Path("/b"), Uri("http://localhost/a/b")),
      (Uri("http://localhost/a/"), Path("b"), Uri("http://localhost/a/b")),
      (Uri("http://localhost/"), "b" / "c", Uri("http://localhost/b/c")),
      (Uri("http://localhost"), "b" / "c", Uri("http://localhost/b/c"))
    )
    "Append a path without adding double slash" in {
      forAll(list) {
        case (base, path, result) => base.append(path) shouldEqual result
      }
    }
  }

}
