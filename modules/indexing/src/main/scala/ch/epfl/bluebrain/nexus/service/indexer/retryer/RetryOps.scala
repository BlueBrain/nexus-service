package ch.epfl.bluebrain.nexus.service.indexer.retryer

import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object RetryOps {

  /**
    * Execute a [[Task]] and provide a retry mechanism on failures of any subtype of [[RetriableErr]] and when the provided
    * partial function ''pf'' is not defined. The response is transformed to ''B'' with the given ''pf''.
    *
    * @param source     the [[Task]] where to add retry support
    * @param pf         the partial function that transforms ''A'' to ''B''
    * @param maxRetries the max number of retries
    * @param strategy   the delay strategy between retries
    * @tparam A the generic type of the [[Task]]s request
    * @tparam B the generic type of the [[Task]]s result
    */
  def retryWhenNot[A, B](source: => Task[A],
                         pf: PartialFunction[A, B],
                         maxRetries: Int,
                         strategy: RetryStrategy): Task[B] = {
    def inner(retry: Int, currentDelay: FiniteDuration): Task[B] = {
      def retryCountDown(ex: Throwable): Task[B] =
        if (retry > 0) inner(retry - 1, strategy.next(currentDelay)).delayExecution(currentDelay)
        else Task.raiseError(ex)

      source
        .flatMap {
          pf.lift(_)
            .map(Task.pure)
            .getOrElse(retryCountDown(TooManyRetries))
        }
        .onErrorHandleWith {
          case ex: RetriableErr => retryCountDown(ex)
          case ex               => Task.raiseError(ex)
        }
    }
    inner(maxRetries, strategy.init)
  }

  /**
    * Execute a [[Future]] and provide a retry mechanism on failures of any subtype of [[RetriableErr]] and when the provided
    * partial function ''pf'' is not defined. The response is transformed to ''B'' with the given ''pf''.
    *
    * @param source     the [[Future]] where to add retry support
    * @param pf         the partial function that transforms ''A'' to ''B''
    * @param maxRetries the max number of retries
    * @param strategy   the delay strategy between retries
    * @tparam A the generic type of the [[Future]]s request
    * @tparam B the generic type of the [[Future]]s result
    */
  def retryWhenNot[A, B](source: () => Future[A], pf: PartialFunction[A, B], maxRetries: Int, strategy: RetryStrategy)(
      implicit ec: ExecutionContext): Future[B] = {
    val s           = Task.deferFuture(source())
    implicit val sc = Scheduler(ec)
    retryWhenNot(s, pf, maxRetries, strategy).runAsync
  }

  /**
    * Execute a [[Task]] and provide a retry mechanism on failures of any subtype of [[RetriableErr]].
    *
    * @param source     the [[Task]] where to add retry support
    * @param maxRetries the max number of retries
    * @param strategy   the delay strategy between retries
    * @tparam A the generic type of the [[Task]]s result
    */
  def retry[A](source: => Task[A], maxRetries: Int, strategy: RetryStrategy): Task[A] =
    retryWhenNot(source, { case i => i: A }: PartialFunction[A, A], maxRetries, strategy)

  /**
    * Execute a [[Future]] and provide a retry mechanism on failures of any subtype of [[RetriableErr]].
    *
    * @param source     the [[Future]] function where to add retry support
    * @param maxRetries the max number of retries
    * @param strategy   the delay strategy between retries
    * @tparam A the generic type of the [[Future]]s result
    */
  def retry[A](source: () => Future[A], maxRetries: Int, strategy: RetryStrategy)(
      implicit ec: ExecutionContext): Future[A] = {
    val s           = Task.deferFuture(source())
    implicit val sc = Scheduler(ec)
    retry(s, maxRetries, strategy).runAsync
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

/**
  * Signals a retry error
  */
final case object TooManyRetries extends Exception
