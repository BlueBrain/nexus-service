package ch.epfl.bluebrain.nexus.service.indexer.retryer

import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random

object RetryOps {

  /**
    * Execute a [[Future]] and provide a retry mechanism on failures of any subtype of [[RetriableErr]].
    *
    * @param source     the [[Future]] function where to add retry support
    * @param maxRetries the max number of retries
    * @param strategy      the delay strategy between retries
    * @tparam A the generic type of the [[Future]]s result
    */
  def retry[A](source: () => Future[A], maxRetries: Int, strategy: RetryStrategy)(
      implicit ec: ExecutionContext): Future[A] = {
    val s = Task.deferFuture {
      source()
    }
    implicit val sc = Scheduler(ec)

    def inner(retry: Int, currentDelay: FiniteDuration): Task[A] = {
      s.onErrorHandleWith {
        case ex: RetriableErr =>
          if (retry > 0)
            inner(retry - 1, strategy.next(currentDelay)).delayExecution(currentDelay)
          else
            Task.raiseError(ex)
        case ex => Task.raiseError(ex)
      }
    }

    inner(maxRetries, strategy.init).runAsync
  }

  /**
    * Interface syntax to expose new functionality into () => Future[A]
    *
    * @param source the [[Future]] function where to add retry support
    * @param ec     the implicitly available [[ExecutionContext]]
    * @tparam A the generic type of the [[Future]]s result
    */
  final implicit class Retryable[A](val source: () => Future[A])(implicit ec: ExecutionContext) {

    /**
      * Method exposed on () => Future[A] instances
      *
      * @param maxRetries the max number of retries
      * @param strategy   the implicitly available retry strategy between retry delays
      * @return an optional Json which contains only the filtered shape.
      */
    def retry(maxRetries: Int)(implicit strategy: RetryStrategy): Future[A] =
      RetryOps.retry(source, maxRetries, strategy)
  }
}

trait RetryStrategy extends Product with Serializable {

  /**
    * Given a current delay value provides the next delay
    * @param current the current delay
    */
  def next(current: FiniteDuration): FiniteDuration

  /**
    * The initial delay
    */
  def init: FiniteDuration
}

object RetryStrategy {

  /**
    * An exponential backoff delay increment strategy.
    *
    * @param max          the maximum delay accepted
    * @param randomFactor the random variation on delay
    */
  final case class Backoff(max: FiniteDuration, randomFactor: Double) extends RetryStrategy {
    require(randomFactor >= 0.0 && randomFactor <= 1.0)
    override def next(current: FiniteDuration): FiniteDuration = {
      val minJitter = 1 - randomFactor
      val maxJitter = 1 + randomFactor
      val nextDelay = 2 * (minJitter + (maxJitter - minJitter) * Random.nextDouble) * current
      nextDelay.min(max).toMillis millis
    }

    override def init: FiniteDuration = next(500 millis)
  }

  /**
    * A linear delay increment strategy
    *
    * @param max       the maximum delay accepted
    * @param increment the linear increment on delay
    */
  final case class Linear(max: FiniteDuration, increment: FiniteDuration = 1 second) extends RetryStrategy {
    override def next(current: FiniteDuration): FiniteDuration =
      (current + increment).min(max)

    override def init: FiniteDuration = increment min max
  }
}
