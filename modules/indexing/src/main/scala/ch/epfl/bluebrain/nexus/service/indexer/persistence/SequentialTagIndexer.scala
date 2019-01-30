package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.event.{Logging, LoggingAdapter}
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, PersistenceQuery}
import akka.stream.scaladsl.{Flow, Source}
import cats.implicits._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.IndexerConfig.{IndexConfigFlow, IndexConfigFunction}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.service.indexer.stream.{SingletonStreamCoordinator, StreamCoordinator}
import ch.epfl.bluebrain.nexus.sourcing.akka.Retry
import ch.epfl.bluebrain.nexus.sourcing.akka.syntax._
import io.circe.Encoder
import monix.eval.Task
import monix.execution.Scheduler
import shapeless.Typeable

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

  private[persistence] class VolatileSourceBuilder[T, E](private val config: IndexerConfig[T, E, Volatile])(
      implicit as: ActorSystem,
      sc: Scheduler) {
    private implicit val log                   = Logging(as, SequentialTagIndexer.getClass)
    private implicit val retry: Retry[Task, E] = config.retry

    def prepareInit: Task[Offset] = config.init.retry *> Task.pure(NoOffset)

    private def toFlow: Graph[T] =
      config match {
        case c: IndexConfigFunction[T, E, Volatile] =>
          Flow[OffsetEvts[T]].mapAsync(1) {
            case OffsetEvts(off, events) => c.index(events.map(_.value)).retry.map(_ => off).runToFuture
          }
        case c: IndexConfigFlow[T, E, Volatile] => c.flow
      }

    def source(implicit T: Typeable[T]): Offset => Source[Offset, NotUsed] =
      SequentialTagIndexer.source(config.pluginId, config.tag, config.batch, config.batchTo, toFlow)
  }

  private[persistence] class PersistentSourceBuilder[T, E](config: IndexerConfig[T, E, Persist])(
      implicit as: ActorSystem,
      sc: Scheduler,
      E: Encoder[T]) {
    private implicit val log                          = Logging(as, SequentialTagIndexer.getClass)
    private val failureLog: IndexFailuresLog[Task]    = IndexFailuresLog(config.name)
    private val projection: ResumableProjection[Task] = ResumableProjection(config.name)
    private implicit val retry: Retry[Task, E]        = config.retry

    def prepareInit: Task[Offset] =
      if (config.storage.restart)
        config.init.retry *> Task.pure(NoOffset)
      else
        config.init.retry.flatMap(_ => projection.fetchLatestOffset.retry)

    private def toFlow: Graph[T] =
      config match {
        case c: IndexConfigFunction[T, E, Persist] =>
          Flow[OffsetEvts[T]].mapAsync(1) {
            case OffsetEvts(off, events) =>
              c.index(events.map(_.value))
                .retry
                .recoverWith {
                  case err =>
                    Task.sequence(events.map { el =>
                      log.error(err,
                                "Indexing event with id '{}' and value '{}' failed'{}'",
                                el.persistenceId,
                                el.value,
                                err.getMessage)
                      failureLog.storeEvent(el.persistenceId, off, el.value)
                    }) *> Task.unit
                }
                .map(_ => off)
                .runToFuture
          }
        case c: IndexConfigFlow[T, E, Persist] => c.flow
      }

    def source(implicit T: Typeable[T]): Offset => Source[Unit, NotUsed] =
      (offset: Offset) =>
        SequentialTagIndexer
          .source(config.pluginId, config.tag, config.batch, config.batchTo, toFlow)
          .apply(offset)
          .mapAsync(1) { offset =>
            log.info("Storing latest offset '{}'", offset)
            projection.storeLatestOffset(offset).runToFuture
        }
  }

  /**
    * Generic tag indexer that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are not persisted once computed the index function or flow.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    * @tparam T the event type
    */
  final def start[T: Typeable, E](config: IndexerConfig[T, E, Volatile])(implicit as: ActorSystem,
                                                                         sc: Scheduler): ActorRef = {
    val builder = new VolatileSourceBuilder(config)
    StreamCoordinator.start(builder.prepareInit, builder.source, config.name)
  }

  /**
    * Generic tag indexer that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are persisted once computed the index function or flow.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    * @tparam T the event type
    */
  final def start[T: Typeable, E](
      config: IndexerConfig[T, E, Persist])(implicit as: ActorSystem, sc: Scheduler, E: Encoder[T]): ActorRef = {
    val builder = new PersistentSourceBuilder(config)
    SingletonStreamCoordinator.start(builder.prepareInit, builder.source, config.name)
  }

  // $COVERAGE-ON$
}
