package ch.epfl.bluebrain.nexus.service.indexer.persistence

import java.util.concurrent.TimeUnit.MILLISECONDS

import akka.NotUsed
import akka.actor.ActorSystem
import cats.MonadError
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer.Graph
import ch.epfl.bluebrain.nexus.sourcing.akka.RetryStrategy.Linear
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.RetryStrategyConfig
import ch.epfl.bluebrain.nexus.sourcing.akka.{Retry, RetryStrategy}
import com.github.ghik.silencer.silent
import monix.eval.Task
import pureconfig.generic.auto._
import pureconfig.loadConfigOrThrow

import scala.concurrent.duration._

/**
  * Enumeration of configuration types.
  *
  * @tparam T the event type
  * @tparam O the type of [[OffsetStorage]]
  */
sealed trait IndexerConfig[T, E, O] {
  def name: String
  def tag: String
  def pluginId: String
  def init: Task[Unit]
  def batch: Int
  def batchTo: FiniteDuration
  def storage: O
  def retry: Retry[Task, E]
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
  private[IndexerConfig] final case class IndexConfigBuilder[T, ST, SP, SN, SI, E, O <: OffsetStorage](
      tag: Option[String] = None,
      plugin: Option[String] = None,
      name: Option[String] = None,
      index: Option[Either[List[T] => Task[Unit], Graph[T]]] = None,
      init: Task[Unit] = Task.unit,
      batch: Int = 1,
      batchTo: FiniteDuration = 50 millis,
      strategy: RetryStrategy = Linear(0 millis, 2000 hours),
      error: MonadError[Task, E],
      storage: O) {

    private implicit val F = error

    @silent
    def build(implicit e1: ST =:= Ready, e2: SP =:= Ready, e3: SN =:= Ready, e4: SI =:= Ready): IndexerConfig[T, E, O] =
      (tag, plugin, name, index) match {
        case (Some(t), Some(p), Some(n), Some(Right(flow))) =>
          IndexConfigFlow(t, p, n, flow, init, batch, batchTo, Retry(strategy), storage)
        case (Some(t), Some(p), Some(n), Some(Left(i))) =>
          IndexConfigFunction(t, p, n, i, init, batch, batchTo, Retry(strategy), storage)
        case _ => throw new RuntimeException("Unexpected: some of the required fields are not set")
      }

    def tag(value: String): IndexConfigBuilder[T, Ready, SP, SN, SI, E, O] =
      copy(tag = Some(value))

    def plugin(value: String): IndexConfigBuilder[T, ST, Ready, SN, SI, E, O] =
      copy(plugin = Some(value))

    def name(value: String): IndexConfigBuilder[T, ST, SP, Ready, SI, E, O] =
      copy(name = Some(value))

    def index[EE](value: List[EE] => Task[Unit]): IndexConfigBuilder[EE, ST, SP, SN, Ready, E, O] =
      copy(index = Some(Left(value)))

    def flow[EE](value: Graph[EE]): IndexConfigBuilder[EE, ST, SP, SN, Ready, E, O] =
      copy(index = Some(Right(value)))

    def init(value: Task[Unit]): IndexConfigBuilder[T, ST, SP, SN, SI, E, O] =
      copy(init = value)

    def offset[S <: OffsetStorage](@silent value: S): IndexConfigBuilder[T, ST, SP, SN, SI, E, S] =
      copy(storage = value)

    def batch(value: Int): IndexConfigBuilder[T, ST, SP, SN, SI, E, O] =
      copy(batch = value)

    def batch(value: Int, timeout: FiniteDuration): IndexConfigBuilder[T, ST, SP, SN, SI, E, O] =
      copy(batch = value, batchTo = timeout)

    def retry[EE](strategy: RetryStrategy)(
        implicit EE: MonadError[Task, EE]): IndexConfigBuilder[T, ST, SP, SN, SI, EE, O] =
      copy(error = EE, strategy = strategy)

    def restart(value: Boolean)(implicit @silent ev: O =:= Persist): IndexConfigBuilder[T, ST, SP, SN, SI, E, Persist] =
      copy(storage = Persist(value))

  }

  private val taskMonadError = implicitly[MonadError[Task, Throwable]]

  /**
    * Retrieves the [[IndexConfigBuilder]] with the default pre-filled arguments.
    */
  final lazy val builder: IndexConfigBuilder[NotUsed, _, _, _, _, Throwable, Persist] =
    IndexConfigBuilder(storage = Persist(restart = false), error = taskMonadError)

  /**
    * Constructs a new [[IndexConfigBuilder]] with some of the arguments pre-filled with the ''as'' configuration
    *
    * @param as the [[ActorSystem]]
    */
  final def fromConfig(implicit as: ActorSystem): IndexConfigBuilder[NotUsed, _, _, _, _, Throwable, Persist] = {
    val config                           = as.settings.config.getConfig("indexing")
    val timeout                          = FiniteDuration(config.getDuration("batch-timeout", MILLISECONDS), MILLISECONDS)
    val chunk                            = config.getInt("batch-chunk")
    val retryConfig: RetryStrategyConfig = loadConfigOrThrow[RetryStrategyConfig](config, "retry")
    builder.retry(retryConfig.retryStrategy).batch(chunk, timeout)
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
    * @param retry the retry strategy
    * @param storage  the [[OffsetStorage]]
    * @tparam T the event type
    * @tparam O the type of [[OffsetStorage]]
    */
  final case class IndexConfigFlow[T, E, O <: OffsetStorage] private (tag: String,
                                                                      pluginId: String,
                                                                      name: String,
                                                                      flow: Graph[T],
                                                                      init: Task[Unit],
                                                                      batch: Int,
                                                                      batchTo: FiniteDuration,
                                                                      retry: Retry[Task, E],
                                                                      storage: O)
      extends IndexerConfig[T, E, O]

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
    * @param retry the retry strategy
    * @param storage  the [[OffsetStorage]]
    * @tparam T the event type
    * @tparam O the type of [[OffsetStorage]]
    */
  final case class IndexConfigFunction[T, E, O <: OffsetStorage] private (tag: String,
                                                                          pluginId: String,
                                                                          name: String,
                                                                          index: List[T] => Task[Unit],
                                                                          init: Task[Unit],
                                                                          batch: Int,
                                                                          batchTo: FiniteDuration,
                                                                          retry: Retry[Task, E],
                                                                          storage: O)
      extends IndexerConfig[T, E, O]

}
