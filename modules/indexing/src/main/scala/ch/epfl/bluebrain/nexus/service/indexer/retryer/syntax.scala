package ch.epfl.bluebrain.nexus.service.indexer.retryer

import monix.eval.Task

import scala.concurrent.{ExecutionContext, Future}

object syntax {

  implicit final def retryFutureSyntax[A](source: () => Future[A])(
      implicit ec: ExecutionContext): RetryableFutureSyntax[A] =
    new RetryableFutureSyntax(source)
  implicit final def retryTaskSyntax[A](source: Task[A]): RetryableTaskSyntax[A] =
    new RetryableTaskSyntax(source)
}

/**
  * Interface syntax to expose new functionality into () => Future[A]
  *
  * @param source the [[Future]] function where to add retry support
  * @param ec     the implicitly available [[ExecutionContext]]
  * @tparam A the generic type of the [[Future]]s result
  */
final class RetryableFutureSyntax[A](private val source: () => Future[A])(implicit ec: ExecutionContext) {

  /**
    * Method exposed on () => Future[A] instances
    *
    * @param maxRetries the max number of retries
    * @param strategy   the implicitly available retry strategy between retry delays
    */
  def retry(maxRetries: Int = Integer.MAX_VALUE)(implicit strategy: RetryStrategy): Future[A] =
    RetryOps.retry(source, maxRetries, strategy)

  /**
    * Method exposed on Future[A] instances
    *
    * @param pf         the partial function that transforms ''A'' to ''B''
    * @param maxRetries the max number of retries
    * @param strategy   the implicitly available retry strategy between retry delays
    * @tparam B the generic type of the [[Future]]s response
    */
  def retryWhenNot[B](pf: PartialFunction[A, B], maxRetries: Int = Integer.MAX_VALUE)(
      implicit strategy: RetryStrategy): Future[B] =
    RetryOps.retryWhenNot(source, pf, maxRetries, strategy)
}

/**
  * Interface syntax to expose new functionality into Task[A]
  *
  * @param source the [[Task]] where to add retry support
  * @tparam A the generic type of the [[Task]]s request
  */
final class RetryableTaskSyntax[A](private val source: Task[A]) extends AnyVal {

  /**
    * Method exposed on Task[A] instances
    *
    * @param maxRetries the max number of retries
    * @param strategy   the implicitly available retry strategy between retry delays
    */
  def retry(maxRetries: Int = Integer.MAX_VALUE)(implicit strategy: RetryStrategy): Task[A] =
    RetryOps.retry(source, maxRetries, strategy)

  /**
    * Method exposed on Task[A] instances
    *
    * @param pf         the partial function that transforms ''A'' to ''B''
    * @param maxRetries the max number of retries
    * @param strategy   the implicitly available retry strategy between retry delays
    * @tparam B the generic type of the [[Task]]s response
    */
  def retryWhenNot[B](pf: PartialFunction[A, B], maxRetries: Int = Integer.MAX_VALUE)(
      implicit strategy: RetryStrategy): Task[B] =
    RetryOps.retryWhenNot(source, pf, maxRetries, strategy)
}
