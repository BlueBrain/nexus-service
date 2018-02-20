package ch.epfl.bluebrain.nexus.service.http.client

import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import ch.epfl.bluebrain.nexus.service.http.client.HttpClient._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class HttpClientSpec extends TestKit(ActorSystem("HttpClientSpec")) with WordSpecLike with Matchers with ScalaFutures {

  override implicit val patienceConfig = PatienceConfig(3 seconds, 100 millis)

  "An HttpClient" should {
    implicit val mt = ActorMaterializer()
    implicit val ec = system.dispatcher
    implicit val cl = akkaHttpClient

    "discard entity when expected HTTP codes in HttpResponse" in {
      val count = new AtomicLong(0L)
      Future(HttpResponse(OK)).discardOnCodesOr(Set(OK)) { _ =>
        val _ = count.incrementAndGet()
        Future.successful(())
      }.futureValue shouldEqual (())
      count.get() shouldEqual 0L
    }

    "execute else block when not unexpected HTTP codes in HttpResponse" in {
      val count = new AtomicLong(0L)
      Future(HttpResponse(NotFound)).discardOnCodesOr(Set(OK)) { _ =>
        val _ = count.incrementAndGet()
        Future.successful(())
      }.futureValue shouldEqual (())
      count.get() shouldEqual 1L
    }
  }

}
