package ch.epfl.bluebrain.nexus.service.indexer.retryer

import akka.actor.ActorSystem
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryOps._
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryOpsSpec.SomeError
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.{Backoff, Linear}
import monix.execution.Scheduler.Implicits.global
import monix.execution.atomic.AtomicLong
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.service.indexer.retryer.syntax._
import monix.eval.Task

import scala.concurrent.Future
import scala.concurrent.duration._

class RetryOpsSpec
    extends TestKit(ActorSystem("TaskRetrySpec"))
    with WordSpecLike
    with Matchers
    with Inspectors
    with Eventually
    with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(20 seconds, 400 millis)

  abstract class Context {
    val count = AtomicLong(0L)
    val future = () =>
      Future[Unit] {
        count.incrementAndGet()
        throw SomeError
    }
    val task = Task.deferFuture(future())
  }

  abstract class Condition(passWhen: Long) {
    val count = AtomicLong(0L)
    val future = () =>
      Future[Option[Long]] {
        count.incrementAndGet() match {
          case i if i < passWhen => None
          case i                 => Some(i)
        }
    }
    val task = Task.deferFuture(future())
  }

  "A RetryOps" when {

    "dealing with Future" should {

      "retry exponentially when it fails" in new Context {
        retry(future, 3, Backoff(5 seconds, 0)).failed.futureValue(timeout(Span(7, Seconds))) shouldBe SomeError
        count.get shouldEqual 4
      }

      "retry exponentially when it fails capped to 1 second" in new Context {
        private implicit val backoff = Backoff(1 seconds, 0)
        future.retry(3).failed.futureValue(timeout(Span(3, Seconds))) shouldBe SomeError
        count.get shouldEqual 4
      }

      "retry linearly when it fails" in new Context {
        retry(future, 4, Linear(5 seconds, 300 millis)).failed
          .futureValue(timeout(Span(3, Seconds))) shouldBe SomeError
        count.get shouldEqual 5
      }

      "retry linearly when it fails capped to 200 mills" in new Context {
        retry(future, 5, Linear(200 millis, 200 millis)).failed
          .futureValue(timeout(Span(1, Seconds))) shouldBe SomeError
        count.get shouldEqual 6
      }

      "don't retry when having a successful future" in new Context {
        val f = () => Future[Long](count.incrementAndGet())
        retry(f, 1, Backoff(5 seconds, 0.2)).futureValue shouldEqual 1L
      }

      "retry when the condition is not satisfied" in new Condition(2) {
        private implicit val backoff = Backoff(1 seconds, 0)
        future.retryWhenNot { case Some(a) => a }.futureValue shouldEqual 2L
        count.get shouldEqual 2
      }
    }

    "dealing with Task" should {

      "retry exponentially when it fails capped to 1 second" in new Context {
        private implicit val backoff = Backoff(1 seconds, 0)
        task.retry(3).runAsync.failed.futureValue(timeout(Span(3, Seconds))) shouldBe SomeError
        count.get shouldEqual 4
      }

      "retry when the condition is not satisfied" in new Condition(3) {
        private implicit val backoff = Backoff(1 seconds, 0)
        task.retryWhenNot { case Some(a) => a }.runAsync.futureValue shouldEqual 3L
        count.get shouldEqual 3
      }
    }
  }
}

object RetryOpsSpec {
  case object SomeError extends RetriableErr("some error")
}
