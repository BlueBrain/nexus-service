package ch.epfl.bluebrain.nexus.service.indexer.persistence

import java.util.concurrent.TimeUnit.{MILLISECONDS, SECONDS}

import akka.NotUsed
import akka.actor.ActorSystem
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer.Graph
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.{Backoff, Linear}
import com.github.ghik.silencer.silent

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Enumeration of configuration types.
  *
  * @tparam T the event type
  * @tparam O the type of [[OffsetStorage]]
  */
sealed trait IndexerConfig[T, O] {
  def name: String
  def tag: String
  def pluginId: String
  def init: () => Future[Unit]
  def strategy: RetryStrategy
  def batch: Int
  def batchTo: FiniteDuration
  def storage: O
}

/**
  *
  * Enumeration of offset storage types.
  */
sealed trait OffsetStorage

object OffsetStorage {

  /**
    * The offset is persisted and the failures get logged.
    *
    * @param restart flag to control from where to start consuming messages on boot.
    *                If set to true, it will start consuming messages from the beginning.
    *                If set to false, it will attempt to resume from the previously stored offset (if any)
    */
  final case class Persist(restart: Boolean) extends OffsetStorage

  /**
    * The offset is NOT persisted and the failures do not get logged.
    */
  final case object Volatile extends OffsetStorage

  type Volatile = Volatile.type
}

object IndexerConfig {

  @SuppressWarnings(Array("LonelySealedTrait"))
  private sealed trait Ready

  @SuppressWarnings(Array("UnusedMethodParameter"))
  private[IndexerConfig] final case class IndexConfigBuilder[T, ST, SP, SN, SI, O <: OffsetStorage](
      tag: Option[String] = None,
      plugin: Option[String] = None,
      name: Option[String] = None,
      index: Option[Either[List[T] => Future[Unit], Graph[T]]] = None,
      init: () => Future[Unit] = () => Future(()),
      batch: Int = 1,
      batchTo: FiniteDuration = 50 millis,
      retries: Int = 1,
      strategy: RetryStrategy = Linear(0 seconds),
      storage: O) {

    @silent
    def build(implicit e1: ST =:= Ready, e2: SP =:= Ready, e3: SN =:= Ready, e4: SI =:= Ready): IndexerConfig[T, O] =
      (tag, plugin, name, index) match {
        case (Some(t), Some(p), Some(n), Some(Right(flow))) =>
          IndexConfigFlow(t, p, n, flow, init, batch, batchTo, retries, strategy, storage)
        case (Some(t), Some(p), Some(n), Some(Left(i))) =>
          IndexConfigFunction(t, p, n, i, init, batch, batchTo, retries, strategy, storage)
        case _ => throw new RuntimeException("Unexpected: some of the required fields are not set")
      }

    def tag(value: String): IndexConfigBuilder[T, Ready, SP, SN, SI, O] =
      copy(tag = Some(value))

    def plugin(value: String): IndexConfigBuilder[T, ST, Ready, SN, SI, O] =
      copy(plugin = Some(value))

    def name(value: String): IndexConfigBuilder[T, ST, SP, Ready, SI, O] =
      copy(name = Some(value))

    def index[E](value: List[E] => Future[Unit]): IndexConfigBuilder[E, ST, SP, SN, Ready, O] =
      copy(index = Some(Left(value)))

    def flow[E](value: Graph[E]): IndexConfigBuilder[E, ST, SP, SN, Ready, O] =
      copy(index = Some(Right(value)))

    def init(value: () => Future[Unit]): IndexConfigBuilder[T, ST, SP, SN, SI, O] =
      copy(init = value)

    def offset[S <: OffsetStorage](@silent value: S): IndexConfigBuilder[T, ST, SP, SN, SI, S] =
      copy(storage = value)

    def batch(value: Int): IndexConfigBuilder[T, ST, SP, SN, SI, O] = copy(batch = value)

    def batch(value: Int, timeout: FiniteDuration): IndexConfigBuilder[T, ST, SP, SN, SI, O] =
      copy(batch = value, batchTo = timeout)

    def retry(times: Int, strategy: RetryStrategy): IndexConfigBuilder[T, ST, SP, SN, SI, O] =
      copy(retries = times, strategy = strategy)

    def restart(value: Boolean)(implicit @silent ev: O =:= Persist): IndexConfigBuilder[T, ST, SP, SN, SI, Persist] =
      copy(storage = Persist(value))

  }

  /**
    * Retrieves the [[IndexConfigBuilder]] with the default pre-filled arguments.
    */
  final lazy val builder: IndexConfigBuilder[NotUsed, _, _, _, _, Persist] =
    IndexConfigBuilder(storage = Persist(restart = false))

  /**
    * Constructs a new [[IndexConfigBuilder]] with some of the arguments pre-filled with the ''as'' configuration
    *
    * @param as the [[ActorSystem]]
    */
  final def fromConfig(implicit as: ActorSystem): IndexConfigBuilder[NotUsed, _, _, _, _, Persist] = {
    val config      = as.settings.config.getConfig("indexing")
    val timeout     = FiniteDuration(config.getDuration("batch-timeout", MILLISECONDS), MILLISECONDS)
    val retryConfig = config.getConfig("retry")
    val retries     = retryConfig.getInt("max-count")
    val strategy =
      Backoff(Duration(retryConfig.getDuration("max-duration", SECONDS), SECONDS),
              retryConfig.getDouble("random-factor"))
    IndexConfigBuilder(retries = retries, strategy = strategy, batchTo = timeout, storage = Persist(restart = false))
  }

  /**
    * Configuration to instrument a [[SequentialTagIndexer]] using a flow.
    *
    * @param tag      the tag to use while selecting the events from the store
    * @param pluginId the persistence query plugin id
    * @param name     the name of this indexer
    * @param flow     the flow that will be inserted into the processing graph
    * @param init     an initialization function that is run before the indexer is (re)started
    * @param batch    the number of events to be grouped
    * @param batchTo  the timeout for the grouping on batches.
    *                 Batching will the amount of time ''batchTo'' to have ''batch'' number of events
    * @param retries  the number of retries on the indexing function
    * @param strategy the retry strategy
    * @param storage  the [[OffsetStorage]]
    * @tparam T the event type
    * @tparam O the type of [[OffsetStorage]]
    */
  final case class IndexConfigFlow[T, O <: OffsetStorage] private (tag: String,
                                                                   pluginId: String,
                                                                   name: String,
                                                                   flow: Graph[T],
                                                                   init: () => Future[Unit],
                                                                   batch: Int,
                                                                   batchTo: FiniteDuration,
                                                                   retries: Int,
                                                                   strategy: RetryStrategy,
                                                                   storage: O)
      extends IndexerConfig[T, O]

  /**
    * Configuration to instrument a [[SequentialTagIndexer]] using am index function.
    *
    * @param tag      the tag to use while selecting the events from the store
    * @param pluginId the persistence query plugin id
    * @param name     the name of this indexer
    * @param index    the indexing function
    * @param init     an initialization function that is run before the indexer is (re)started
    * @param batch    the number of events to be grouped
    * @param batchTo  the timeout for the grouping on batches.
    *                 Batching will the amount of time ''batchTo'' to have ''batch'' number of events
    * @param retries  the number of retries on the indexing function
    * @param strategy the retry strategy
    * @param storage  the [[OffsetStorage]]
    * @tparam T the event type
    * @tparam O the type of [[OffsetStorage]]
    */
  final case class IndexConfigFunction[T, O <: OffsetStorage] private (tag: String,
                                                                       pluginId: String,
                                                                       name: String,
                                                                       index: List[T] => Future[Unit],
                                                                       init: () => Future[Unit],
                                                                       batch: Int,
                                                                       batchTo: FiniteDuration,
                                                                       retries: Int,
                                                                       strategy: RetryStrategy,
                                                                       storage: O)
      extends IndexerConfig[T, O]

}
