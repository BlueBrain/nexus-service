package ch.epfl.bluebrain.nexus.service.indexer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, PersistenceQuery}
import akka.stream.scaladsl.Source
import cats.effect.Effect
import cats.implicits._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.service.indexer.persistence._
import ch.epfl.bluebrain.nexus.sourcing.akka.Retry
import ch.epfl.bluebrain.nexus.sourcing.akka.syntax._
import io.circe.Encoder
import shapeless.Typeable

/**
  *
  * Enumeration of stream by tag types.
  */
sealed trait StreamByTag[F[_], A] {

  /**
    * @return an initialization function that fetches the value required to initialize the source
    */
  def fetchInit: F[A]

  /**
    * A source generated from the  provided init value
    *
    * @param init initialization value
    */
  def source(init: A): Source[A, _]
}

/**
  * An event with its persistenceId
  *
  * @param persistenceId the event persistenceId
  * @param value         the event value
  * @tparam T the event type
  */
final case class WrappedEvt[T](persistenceId: String, value: T)

/**
  * A sequence of events with an offset
  *
  * @param offset the offset value
  * @param events the sequence of events
  * @tparam T the event type
  */
final case class OffsetEvtBatch[T](offset: Offset, events: List[WrappedEvt[T]])

object OffsetEvtBatch {
  def empty[T]: OffsetEvtBatch[T] = OffsetEvtBatch(Offset.noOffset, List.empty[WrappedEvt[T]])
}

object StreamByTag {

  abstract class BatchedStreamByTag[F[_], Event, MappedEvt, Err, O <: OffsetStorage](
      config: IndexerConfig[F, Event, MappedEvt, Err, O])(implicit F: Effect[F], T: Typeable[Event], as: ActorSystem) {

    private[indexer] implicit val log                  = Logging(as, SequentialTagIndexer.getClass)
    private[indexer] implicit val retry: Retry[F, Err] = config.retry
    private[indexer] type IdentifiedEvent = (String, Event, MappedEvt)

    private[indexer] def batchedSource(initialOffset: Offset): Source[(Offset, List[IdentifiedEvent]), NotUsed] = {
      val eventsByTag =
        PersistenceQuery(as).readJournalFor[EventsByTagQuery](config.pluginId).eventsByTag(config.tag, initialOffset)
      val eventsCasted = eventsByTag.flatMapConcat {
        case EventEnvelope(offset, persistenceId, sequenceNr, event) =>
          log.debug("Processing event for persistence id '{}', seqNr '{}'", persistenceId, sequenceNr)
          T.cast(event) match {
            case Some(casted) => Source.single((offset, persistenceId, casted))
            case _ =>
              log.warning("Some of the Events on the list are not compatible with type '{}', skipping...", T.describe)
              Source.empty
          }
      }

      val eventsMapped = eventsCasted
        .mapAsync(1) {
          case (offset, id, event) =>
            val mapped = config
              .mapping(event)
              .retry
              .map {
                case Some(evtMapped) => Some((offset, id, event, evtMapped))
                case None =>
                  log.warning("Indexing event with id '{}' and value '{}' failed '{}'", id, event, "Mapping failed")
                  None
              }
              .recoverWith(logError(id, event))
            F.toIO(mapped).unsafeToFuture()
        }
        .collect { case Some(value) => value }

      val eventsBatched = eventsMapped.groupedWithin(config.batch, config.batchTo).filter(_.nonEmpty).map { batched =>
        val offset = batched.lastOption.map { case (off, _, _, _) => off }.getOrElse(NoOffset)
        (offset, batched.map { case (_, id, event, mappedEvent) => (id, event, mappedEvent) }.toList)
      }
      eventsBatched
    }

    private def logError(id: String,
                         event: Event): PartialFunction[Throwable, F[Option[(Offset, String, Event, MappedEvt)]]] = {
      case err =>
        log.error(err, "Indexing event with id '{}' and value '{}' failed '{}'", id, event, err.getMessage)
        F.pure(None)
    }

  }

  /**
    * Generates a source that reads from PersistenceQuery the events with the provided tag. The progress is stored and the errors logged. The different stages are represented below:
    *
    * +----------------------------+    +--------------+    +--------------+    +---------------+    +---------------+    +----------------------+
    * | eventsByTag(currentOffset) |--->| eventsCasted |--->| eventsMapped |--->| eventsBatched |--->| eventsIndexed |--->| eventsStoredProgress |
    * +----------------------------+    +--------------+    +--------------+    +---------------+    +---------------+    +----------------------+
    *
    */
  final class PersistentStreamByTag[F[_], Event: Encoder, MappedEvt, Err](
      config: IndexerConfig[F, Event, MappedEvt, Err, Persist])(implicit failureLog: IndexFailuresLog[F],
                                                                projection: ResumableProjection[F],
                                                                F: Effect[F],
                                                                T: Typeable[Event],
                                                                as: ActorSystem)
      extends BatchedStreamByTag(config)
      with StreamByTag[F, Offset] {

    def fetchInit: F[Offset] =
      if (config.storage.restart)
        config.init.retry *> F.pure(NoOffset)
      else
        config.init.retry.flatMap(_ => projection.fetchLatestOffset.retry)

    def source(initialOffset: Offset): Source[Offset, NotUsed] = {

      val eventsIndexed = batchedSource(initialOffset).mapAsync(1) {
        case (offset, events) =>
          val index =
            config.index(events.map { case (_, _, mapped) => mapped }).retry.recoverWith(recoverIndex(offset, events))
          F.toIO(index.map(_ => offset)).unsafeToFuture()
      }
      val eventsStoredProgress = eventsIndexed.mapAsync(1) { offset =>
        F.toIO(projection.storeLatestOffset(offset).retry.map(_ => offset)).unsafeToFuture()
      }
      eventsStoredProgress
    }

    private def recoverIndex(offset: Offset, events: List[IdentifiedEvent]): PartialFunction[Throwable, F[Unit]] = {
      case err =>
        events.traverse {
          case (id, event, _) =>
            log.error(err, "Indexing event with id '{}' and value '{}' failed'{}'", id, event, err.getMessage)
            failureLog.storeEvent(id, offset, event)
        } *> F.unit
    }
  }

  /**
    * Generates a source that reads from PersistenceQuery the events with the provided tag. The different stages are represented below:
    *
    * +-----------------------+    +--------------+    +--------------+    +---------------+    +---------------+
    * | eventsByTag(NoOffset) |--->| eventsCasted |--->| eventsMapped |--->| eventsBatched |--->| eventsIndexed |
    * +-----------------------+    +--------------+    +--------------+    +---------------+    +---------------+
    *
    */
  final class VolatileStreamByTag[F[_], Event, MappedEvt, Err](
      config: IndexerConfig[F, Event, MappedEvt, Err, Volatile])(implicit
                                                                 F: Effect[F],
                                                                 T: Typeable[Event],
                                                                 as: ActorSystem)
      extends BatchedStreamByTag(config)
      with StreamByTag[F, Offset] {

    def fetchInit: F[Offset] = config.init.retry *> F.pure(NoOffset)

    def source(initialOffset: Offset): Source[Offset, NotUsed] = {
      val eventsIndexed = batchedSource(initialOffset)
        .mapAsync(1) {
          case (offset, events) =>
            val index =
              config
                .index(events.map { case (_, _, mapped) => mapped })
                .retry
                .recoverWith(logError(events))
                .map(_ => offset)
            F.toIO(index).unsafeToFuture()
        }
      eventsIndexed
    }

    private def logError(events: List[IdentifiedEvent]): PartialFunction[Throwable, F[Unit]] = {
      case err =>
        events.foreach {
          case (id, event, _) =>
            log.error(err, "Indexing event with id '{}' and value '{}' failed '{}'", id, event, err.getMessage)
        }
        F.unit
    }
  }
}
