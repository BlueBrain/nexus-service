package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, PersistenceQuery}
import akka.stream.scaladsl.{Flow, Source}
import cats.implicits._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexerConfig.{IndexConfigFlow, IndexConfigFunction}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.service.indexer.retryer.syntax._
import ch.epfl.bluebrain.nexus.service.indexer.stream.{SingletonStreamCoordinator, StreamCoordinator}
import io.circe.Encoder
import shapeless.Typeable

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * Generic tag indexer that uses the specified resumable projection to iterate over the collection of events selected
  * via the specified tag and apply the argument indexing function.  It starts as a singleton actor in a
  * clustered deployment.  If the event type is not compatible with the events deserialized from the persistence store
  * the events are skipped.
  */
object SequentialTagIndexer {

  /**
    * An event with its persistenceId
    *
    * @param persistenceId the event persistenceId
    * @param value         the event value
    * @tparam T the event type
    */
  final case class WrappedEvt[T](persistenceId: String, value: T)

  /**
    * A list of events with an offset
    *
    * @param offset the offset value
    * @param events the list of events
    * @tparam T the event type
    */
  final case class OffsetEvts[T](offset: Offset, events: List[WrappedEvt[T]])

  object OffsetEvts {
    def empty[T]: OffsetEvts[T] = OffsetEvts(Offset.noOffset, List.empty[WrappedEvt[T]])
  }

  type Graph[T] = Flow[OffsetEvts[T], Offset, NotUsed]

  private def source[T](pluginId: String, tag: String, batch: Int, timeout: FiniteDuration, flow: Graph[T])(
      implicit as: ActorSystem,
      T: Typeable[T],
      log: LoggingAdapter): Offset => Source[Offset, NotUsed] = {
    def castEvents(events: List[EventEnvelope]): Option[OffsetEvts[T]] =
      events
        .foldM(OffsetEvts.empty[T]) {
          case (OffsetEvts(_, acc), EventEnvelope(off, persistenceId, sequenceNr, event)) =>
            log.debug("Processing event for persistence id '{}', seqNr '{}'", persistenceId, sequenceNr)
            T.cast(event).map(v => OffsetEvts(off, WrappedEvt(persistenceId, v) :: acc))
        }
        .map(v => v.copy(events = v.events.reverse))
    (offset: Offset) =>
      PersistenceQuery(as)
        .readJournalFor[EventsByTagQuery](pluginId)
        .eventsByTag(tag, offset)
        .groupedWithin(batch, timeout)
        .filter(_.nonEmpty)
        .flatMapConcat { events =>
          castEvents(events.toList) match {
            case Some(transformed) => Source.single(transformed)
            case _ =>
              log.debug(s"Some of the Events on the list are not compatible with type '${T.describe}, skipping...'")
              Source.empty
          }
        }
        .via(flow)
  }

  private[persistence] implicit class IndexConfigVolatileOps[T](private val config: IndexerConfig[T, Volatile])(
      implicit as: ActorSystem) {
    import as.dispatcher
    private implicit val s   = config.strategy
    private implicit val log = Logging(as, SequentialTagIndexer.getClass)

    def prepareInit: () => Future[Offset] =
      () => config.init().map(_ => NoOffset)

    private def toFlow: Graph[T] =
      config match {
        case c: IndexConfigFunction[T, Volatile] =>
          Flow[OffsetEvts[T]].mapAsync(1) {
            case OffsetEvts(off, events) =>
              (() => c.index(events.map(_.value))).retry(c.retries).map(_ => off)
          }
        case c: IndexConfigFlow[T, Volatile] => c.flow
      }

    def source(implicit T: Typeable[T]): Offset => Source[Offset, NotUsed] =
      SequentialTagIndexer.source(config.pluginId, config.tag, config.batch, config.batchTo, toFlow)
  }

  private[persistence] implicit class IndexConfigPersistOps[T](config: IndexerConfig[T, Persist])(
      implicit as: ActorSystem,
      E: Encoder[T]) {
    import as.dispatcher
    private implicit val s      = config.strategy
    private implicit val log    = Logging(as, SequentialTagIndexer.getClass)
    private lazy val failureLog = IndexFailuresLog(config.name)
    private lazy val projection = ResumableProjection(config.name)

    def prepareInit: () => Future[Offset] =
      if (config.storage.restart)() => config.init().map(_ => NoOffset)
      else
        () => config.init().flatMap(_ => projection.fetchLatestOffset)

    private def toFlow: Graph[T] =
      config match {
        case c: IndexConfigFunction[T, Persist] =>
          Flow[OffsetEvts[T]].mapAsync(1) {
            case OffsetEvts(off, events) =>
              (() => c.index(events.map(_.value)))
                .retry(c.retries)
                .recoverWith {
                  case err =>
                    Future.sequence(events.map { el =>
                      log.error(err,
                                "Indexing event with id '{}' and value '{}' failed'{}'",
                                el.persistenceId,
                                el.value,
                                err.getMessage)
                      failureLog.storeEvent(el.persistenceId, off, el.value)
                    })
                }
                .map(_ => off)
          }
        case c: IndexConfigFlow[T, Persist] => c.flow
      }

    def source(implicit T: Typeable[T]): Offset => Source[Unit, NotUsed] =
      (offset: Offset) =>
        SequentialTagIndexer
          .source(config.pluginId, config.tag, config.batch, config.batchTo, toFlow)
          .apply(offset)
          .mapAsync(1) { offset =>
            log.debug("Storing latest offset '{}'", offset)
            projection.storeLatestOffset(offset)
        }
  }

  /**
    * Generic tag indexer that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are not persisted once computed the index function or flow.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    * @tparam T the event type
    */
  final def start[T](config: IndexerConfig[T, Volatile])(implicit as: ActorSystem, T: Typeable[T]) =
    StreamCoordinator.start(config.prepareInit, config.source, config.name)

  /**
    * Generic tag indexer that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are persisted once computed the index function or flow.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    * @tparam T the event type
    */
  final def start[T](config: IndexerConfig[T, Persist])(implicit as: ActorSystem, T: Typeable[T], E: Encoder[T]) =
    SingletonStreamCoordinator.start(config.prepareInit, config.source, config.name)

  // $COVERAGE-ON$
}
