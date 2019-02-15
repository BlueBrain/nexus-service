package ch.epfl.bluebrain.nexus.service.indexer.persistence

import java.util.concurrent.TimeUnit.MILLISECONDS

import akka.NotUsed
import akka.actor.ActorSystem
import cats.MonadError
import cats.effect.Timer
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.sourcing.akka.RetryStrategy.Linear
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.RetryStrategyConfig
import ch.epfl.bluebrain.nexus.sourcing.akka.{Retry, RetryStrategy}
import com.github.ghik.silencer.silent
import pureconfig.generic.auto._
import pureconfig.loadConfigOrThrow

import scala.concurrent.duration._

/**
  * Configuration to instrument a [[SequentialTagIndexer]] using am index function.
  *
  * @param tag      the tag to use while selecting the events from the store
  * @param pluginId the persistence query plugin id
  * @param name     the name of this indexer
  * @param mapping  the mapping function from Event to MappedEvt
  * @param index    the indexing function
  * @param init     an initialization function that is run before the indexer is (re)started
  * @param batch    the number of events to be grouped
  * @param batchTo  the timeout for the grouping on batches.
  *                 Batching will the amount of time ''batchTo'' to have ''batch'' number of events the retry strategy
  * @param storage  the [[OffsetStorage]]
  * @tparam Event     the event type
  * @tparam MappedEvt the mapped event type
  * @tparam Err       the error type
  * @tparam O         the type of [[OffsetStorage]]
  */
final case class IndexerConfig[F[_], Event, MappedEvt, Err, O <: OffsetStorage] private (
    tag: String,
    pluginId: String,
    name: String,
    mapping: Event => F[Option[MappedEvt]],
    index: List[MappedEvt] => F[Unit],
    init: F[Unit],
    batch: Int,
    batchTo: FiniteDuration,
    retry: Retry[F, Err],
    storage: O)

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
  private[IndexerConfig] final case class IndexConfigBuilder[F[_]: Timer,
                                                             Event,
                                                             MappedEvt,
                                                             Tag,
                                                             Plugin,
                                                             Name,
                                                             Index,
                                                             Mapping,
                                                             Err,
                                                             O <: OffsetStorage](
      tag: Option[String] = None,
      plugin: Option[String] = None,
      name: Option[String] = None,
      mapping: Option[Event => F[Option[MappedEvt]]] = None,
      index: Option[List[MappedEvt] => F[Unit]] = None,
      init: F[Unit],
      batch: Int = 1,
      batchTo: FiniteDuration = 50 millis,
      strategy: RetryStrategy = Linear(0 millis, 2000 hours),
      error: MonadError[F, Err],
      storage: O) {

    private implicit val F = error

    @silent
    def build(implicit e1: Tag =:= Ready,
              e2: Plugin =:= Ready,
              e3: Name =:= Ready,
              e4: Index =:= Ready,
              e5: Mapping =:= Ready): IndexerConfig[F, Event, MappedEvt, Err, O] =
      (tag, plugin, name, index, mapping) match {
        case (Some(t), Some(p), Some(n), Some(i), Some(m)) =>
          IndexerConfig(t, p, n, m, i, init, batch, batchTo, Retry(strategy), storage)
        case _ => throw new RuntimeException("Unexpected: some of the required fields are not set")
      }

    def tag(value: String): IndexConfigBuilder[F, Event, MappedEvt, Ready, Plugin, Name, Index, Mapping, Err, O] =
      copy(tag = Some(value))

    def plugin(value: String): IndexConfigBuilder[F, Event, MappedEvt, Tag, Ready, Name, Index, Mapping, Err, O] =
      copy(plugin = Some(value))

    def name(value: String): IndexConfigBuilder[F, Event, MappedEvt, Tag, Plugin, Ready, Index, Mapping, Err, O] =
      copy(name = Some(value))

    def init(value: F[Unit]): IndexConfigBuilder[F, Event, MappedEvt, Tag, Plugin, Name, Index, Mapping, Err, O] =
      copy(init = value)

    def index(value: List[MappedEvt] => F[Unit])(implicit @silent ev: Mapping =:= Ready)
      : IndexConfigBuilder[F, Event, MappedEvt, Tag, Plugin, Name, Ready, Mapping, Err, O] =
      copy(index = Some(value))

    def mapping[TT, TTO](
        value: TT => F[Option[TTO]]): IndexConfigBuilder[F, TT, TTO, Tag, Plugin, Name, Index, Ready, Err, O] =
      copy(mapping = Some(value), index = None)

    def offset[S <: OffsetStorage](
        value: S): IndexConfigBuilder[F, Event, MappedEvt, Tag, Plugin, Name, Index, Mapping, Err, S] =
      copy(storage = value)

    def batch(value: Int): IndexConfigBuilder[F, Event, MappedEvt, Tag, Plugin, Name, Index, Mapping, Err, O] =
      copy(batch = value)

    def batch(
        value: Int,
        timeout: FiniteDuration): IndexConfigBuilder[F, Event, MappedEvt, Tag, Plugin, Name, Index, Mapping, Err, O] =
      copy(batch = value, batchTo = timeout)

    def retry[EE](strategy: RetryStrategy)(implicit EE: MonadError[F, EE])
      : IndexConfigBuilder[F, Event, MappedEvt, Tag, Plugin, Name, Index, Mapping, EE, O] =
      copy(error = EE, strategy = strategy)

    def restart(value: Boolean)(implicit @silent ev: O =:= Persist)
      : IndexConfigBuilder[F, Event, MappedEvt, Tag, Plugin, Name, Index, Mapping, Err, Persist] =
      copy(storage = Persist(value))

  }

  /**
    * Retrieves the [[IndexConfigBuilder]] with the default pre-filled arguments.
    */
  def builder[F[_]: Timer](implicit F: MonadError[F, Throwable])
    : IndexConfigBuilder[F, NotUsed, NotUsed, _, _, _, _, _, Throwable, Persist] =
    IndexConfigBuilder(storage = Persist(restart = false), error = F, init = F.unit)

  /**
    * Constructs a new [[IndexConfigBuilder]] with some of the arguments pre-filled with the ''as'' configuration
    *
    * @param as the [[ActorSystem]]
    */
  final def fromConfig[F[_]: Timer](
      implicit as: ActorSystem,
      F: MonadError[F, Throwable]): IndexConfigBuilder[F, NotUsed, NotUsed, _, _, _, _, _, Throwable, Persist] = {
    val config                           = as.settings.config.getConfig("indexing")
    val timeout                          = FiniteDuration(config.getDuration("batch-timeout", MILLISECONDS), MILLISECONDS)
    val chunk                            = config.getInt("batch")
    val retryConfig: RetryStrategyConfig = loadConfigOrThrow[RetryStrategyConfig](config, "retry")
    builder.retry(retryConfig.retryStrategy).batch(chunk, timeout)
  }

}
