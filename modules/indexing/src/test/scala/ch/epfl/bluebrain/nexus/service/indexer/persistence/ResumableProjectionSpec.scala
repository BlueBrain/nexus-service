package ch.epfl.bluebrain.nexus.service.indexer.persistence

import java.util.UUID

import akka.persistence.query.{NoOffset, Offset}
import akka.testkit.{TestKit, TestKitBase}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Matchers, WordSpecLike}

import scala.concurrent.duration._

//noinspection TypeAnnotation
@DoNotDiscover
class ResumableProjectionSpec
    extends TestKitBase
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit lazy val system = SystemBuilder.persistence("ResumableProjectionSpec")

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "A ResumableProjection" should {
    val id = UUID.randomUUID().toString

    "store an offset" in {
      ResumableProjection(id).storeLatestOffset(Offset.sequence(42)).futureValue
    }

    "retrieve stored offset" in {
      ResumableProjection(id).fetchLatestOffset.futureValue shouldEqual Offset.sequence(42)
    }

    "retrieve NoOffset for unknown projections" in {
      ResumableProjection(UUID.randomUUID().toString).fetchLatestOffset.futureValue shouldEqual NoOffset
    }
  }

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(30 seconds, 1 second)
}
