package ch.epfl.bluebrain.nexus.service.indexer.cache

import cats.ApplicativeError
import cats.effect.Timer
import ch.epfl.bluebrain.nexus.sourcing.akka.RetryStrategy
import ch.epfl.bluebrain.nexus.sourcing.akka.RetryStrategy._
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.RetryStrategyConfig

import scala.concurrent.duration.FiniteDuration

/**
  * KeyValueStore configuration.
  *
  * @param askTimeout         the maximum duration to wait for the replicator to reply
  * @param consistencyTimeout the maximum duration to wait for a consistent read or write across the cluster
  * @param retry              the retry strategy configuration
  */
final case class KeyValueStoreConfig(
    askTimeout: FiniteDuration,
    consistencyTimeout: FiniteDuration,
    retry: RetryStrategyConfig,
) {

  /**
    * Computes a retry strategy from the provided configuration.
    */
  def retryStrategy[F[_]: Timer, E](implicit F: ApplicativeError[F, E]): RetryStrategy[F] =
    retry.strategy match {
      case "exponential" => exponentialBackoff(retry.initialDelay, retry.maxRetries, retry.factor)
      case "once"        => once
      case _             => never
    }
}
