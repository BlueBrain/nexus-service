package ch.epfl.bluebrain.nexus.service.indexer.persistence

import java.util.UUID

import akka.persistence.query.Offset
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.{TestKit, TestKitBase}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexFailuresStorageSpec.SomeEvent
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Matchers, WordSpecLike}
import monix.execution.Scheduler.Implicits.global
import scala.concurrent.duration._

//noinspection TypeAnnotation
@DoNotDiscover
class IndexFailuresStorageSpec
    extends TestKitBase
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit lazy val system = SystemBuilder.persistence("IndexFailuresStorageSpec")

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "An IndexFailuresLog" should {
    val id            = UUID.randomUUID().toString
    val persistenceId = s"/some/${UUID.randomUUID()}"
    implicit val mt   = ActorMaterializer()

    "store an event" in {
      IndexFailuresLog(id)
        .storeEvent(persistenceId, Offset.sequence(42), SomeEvent(1L, "description"))
        .runToFuture
        .futureValue
    }

    "store another event" in {
      IndexFailuresLog(id)
        .storeEvent(persistenceId, Offset.sequence(1), SomeEvent(2L, "description2"))
        .runToFuture
        .futureValue
    }

    "retrieve stored events" in {
      fetchLogs(id) should contain allElementsOf Seq(SomeEvent(1L, "description"), SomeEvent(2L, "description2"))
    }

    "retrieve empty list of events for unknown failure log" in {
      fetchLogs(UUID.randomUUID().toString) shouldEqual List.empty[SomeEvent]
    }
  }

  private def fetchLogs(id: String)(implicit mt: Materializer) =
    IndexFailuresLog(id).fetchEvents[SomeEvent].runFold(Vector.empty[SomeEvent])(_ :+ _).futureValue

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(30 seconds, 1 second)
}

object IndexFailuresStorageSpec {
  case class SomeEvent(rev: Long, description: String)
}
