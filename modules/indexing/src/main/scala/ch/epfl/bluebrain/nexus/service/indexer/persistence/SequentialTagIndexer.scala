package ch.epfl.bluebrain.nexus.service.indexer.persistence

import java.util.concurrent.TimeUnit._

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, PersistenceQuery}
import akka.stream.scaladsl.{Flow, Source}
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryOps._
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.Backoff
import ch.epfl.bluebrain.nexus.service.indexer.stream.{SingletonStreamCoordinator, StreamCoordinator}
import io.circe.Encoder
import shapeless.Typeable

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * Generic tag indexer that uses the specified resumable projection to iterate over the collection of events selected
  * via the specified tag and apply the argument indexing function.  It starts as a singleton actor in a
  * clustered deployment.  If the event type is not compatible with the events deserialized from the persistence store
  * the events are skipped.
  */
object SequentialTagIndexer {

  private[persistence] def initialize(underlying: () => Future[Unit], id: String)(
      implicit as: ActorSystem): () => Future[Offset] = {
    import as.dispatcher
    val projection = ResumableProjection(id)
    () =>
      underlying().flatMap(_ => projection.fetchLatestOffset)
  }

  private[persistence] def initialize(underlying: () => Future[Unit])(
      implicit as: ActorSystem): () => Future[Offset] = {
    import as.dispatcher
    () =>
      underlying().map(_ => NoOffset)
  }
  private[persistence] def source[T](index: T => Future[Unit], id: String, pluginId: String, tag: String)(
      implicit as: ActorSystem,
      T: Typeable[T],
      E: Encoder[T]): Offset => Source[Unit, NotUsed] = {
    source(toFlow(index, id), id, pluginId, tag)
  }

  private[persistence] def source[T](
      index: Flow[(Offset, String, T), Offset, _],
      id: String,
      pluginId: String,
      tag: String)(implicit as: ActorSystem, T: Typeable[T]): Offset => Source[Unit, NotUsed] = {
    val log        = Logging(as, SequentialTagIndexer.getClass)
    val projection = ResumableProjection(id)
    (offset: Offset) =>
      source(index, pluginId, tag)
        .apply(offset)
        .mapAsync(1) { offset =>
          log.debug("Storing latest offset '{}'", offset)
          projection.storeLatestOffset(offset)
        }
  }

  private[persistence] def source[T](index: Flow[(Offset, String, T), Offset, _], pluginId: String, tag: String)(
      implicit as: ActorSystem,
      T: Typeable[T]): Offset => Source[Offset, NotUsed] = {
    val log = Logging(as, SequentialTagIndexer.getClass)
    (offset: Offset) =>
      PersistenceQuery(as)
        .readJournalFor[EventsByTagQuery](pluginId)
        .eventsByTag(tag, offset)
        .flatMapConcat {
          case EventEnvelope(off, persistenceId, sequenceNr, event) =>
            log.debug("Processing event for persistence id '{}', seqNr '{}'", persistenceId, sequenceNr)
            T.cast(event) match {
              case Some(value) =>
                Source.single((off, persistenceId, value))
              case None =>
                log.debug(s"Event not compatible with type '${T.describe}, skipping...'")
                Source.empty
            }
        }
        .via(index)
  }

  private def lookupRetriesConfig(implicit as: ActorSystem): (Int, RetryStrategy) = {
    val config  = as.settings.config.getConfig("indexing.retry")
    val retries = config.getInt("max-count")
    val backoff =
      Backoff(Duration(config.getDuration("max-duration", SECONDS), SECONDS), config.getDouble("random-factor"))
    (retries, backoff)
  }

  private[persistence] def toFlow[T](index: T => Future[Unit], id: String)(
      implicit as: ActorSystem,
      E: Encoder[T]): Flow[(Offset, String, T), Offset, NotUsed] = {
    import as.dispatcher
    implicit val (retries, backoff) = lookupRetriesConfig
    val failureLog                  = IndexFailuresLog(id)
    Flow[(Offset, String, T)].mapAsync(1) {
      case (off, persistenceId, el) =>
        (() => index(el))
          .retry(retries)
          .recoverWith { case _ => failureLog.storeEvent(persistenceId, off, el) }
          .map(_ => off)
    }
  }

  private[persistence] def toFlow[T](index: T => Future[Unit])(
      implicit as: ActorSystem): Flow[(Offset, String, T), Offset, NotUsed] = {
    import as.dispatcher
    implicit val (retries, backoff) = lookupRetriesConfig
    Flow[(Offset, String, T)].mapAsync(1) {
      case (off, _, el) =>
        (() => index(el))
          .retry(retries)
          .map(_ => off)
    }
  }

  /**
    * Generic tag indexer that uses the specified resumable projection to iterate over the collection of events selected
    * via the specified tag and apply the argument indexing function.  It starts as a singleton actor in a
    * clustered deployment.  If the event type is not compatible with the events deserialized from the persistence store
    * the events are skipped.
    *
    * @param init     an initialization function that is run before the indexer is (re)started
    * @param index    the indexing function
    * @param id       the id of the resumable projection and indexing log to use
    * @param pluginId the persistence query plugin id
    * @param tag      the tag to use while selecting the events from the store
    * @param name     the name of this indexer
    * @param as       an implicitly available actor system
    * @param T        a Typeable instance for the event type T
    * @param E        an implicitly available [[Encoder]] for T
    * @tparam T the event type
    */
  // $COVERAGE-OFF$
  final def start[T](init: () => Future[Unit],
                     index: T => Future[Unit],
                     id: String,
                     pluginId: String,
                     tag: String,
                     name: String)(implicit as: ActorSystem, T: Typeable[T], E: Encoder[T]): ActorRef = {
    SingletonStreamCoordinator.start(initialize(init, id), source(toFlow(index, id), id, pluginId, tag), name)
  }

  /**
    * Generic tag indexer that uses the specified resumable projection to iterate over the collection of events selected
    * via the specified tag and pass it through provided flow. It starts as a singleton actor in a
    * clustered deployment.  If the event type is not compatible with the events deserialized from the persistence store
    * the events are skipped.
    *
    * @param flow     the flow that will be inserted into the processing graph
    * @param id       the id of the resumable projection and indexing log to use
    * @param pluginId the persistence query plugin id
    * @param tag      the tag to use while selecting the events from the store
    * @param name     the name of this indexer
    * @param as       an implicitly available actor system
    * @param T        a Typeable instance for the event type T
    * @tparam T the event type
    */
  final def start[T](flow: Flow[(Offset, String, T), Offset, NotUsed],
                     id: String,
                     pluginId: String,
                     tag: String,
                     name: String)(implicit as: ActorSystem, T: Typeable[T]): ActorRef = {
    SingletonStreamCoordinator.start(initialize(() => Future.successful(()), id), source(flow, id, pluginId, tag), name)
  }

  /**
    * Generic tag indexer that uses the specified resumable projection to iterate over the collection of events selected
    * via the specified tag and apply the argument indexing function.  It starts as a singleton actor in a
    * clustered deployment.  If the event type is not compatible with the events deserialized from the persistence store
    * the events are skipped.
    *
    * @param index    the indexing function
    * @param id       the id of the resumable projection and indexing log to use
    * @param pluginId the persistence query plugin id
    * @param tag      the tag to use while selecting the events from the store
    * @param name     the name of this indexer
    * @param as       an implicitly available actor system
    * @param T        a Typeable instance for the event type T
    * @param E        an implicitly available [[Encoder]] for T
    * @tparam T the event type
    */
  final def start[T](index: T => Future[Unit], id: String, pluginId: String, tag: String, name: String)(
      implicit as: ActorSystem,
      T: Typeable[T],
      E: Encoder[T]): ActorRef =
    start(() => Future.successful(()), index, id, pluginId, tag, name)

  /**
    * Generic tag indexer that iterates over the collection of events selected
    * via the specified tag and apply the argument indexing function.
    * If the event type is not compatible with the events deserialized from the persistence store
    * the events are skipped.
    * @param init     an initialization function that is run before the indexer is (re)started
    * @param index    the indexing function
    * @param pluginId the persistence query plugin id
    * @param tag      the tag to use while selecting the events from the store
    * @param name     the name of this indexer
    * @param as       an implicitly available actor system
    * @param T        a Typeable instance for the event type T
    * @tparam T the event type
    */
  final def startLocal[T](init: () => Future[Unit],
                          index: T => Future[Unit],
                          pluginId: String,
                          tag: String,
                          name: String)(implicit as: ActorSystem, T: Typeable[T]): ActorRef = {
    StreamCoordinator.start(initialize(init), source(toFlow(index), pluginId, tag), name)
  }

  /**
    * Generic tag indexer that iterates over the collection of events selected
    * via the specified tag and pass it through provided flow.
    * If the event type is not compatible with the events deserialized from the persistence store
    * the events are skipped.
    * @param flow     the flow that will be inserted into the processing graph
    * @param pluginId the persistence query plugin id
    * @param tag      the tag to use while selecting the events from the store
    * @param name     the name of this indexer
    * @param as       an implicitly available actor system
    * @param T        a Typeable instance for the event type T
    * @tparam T the event type
    */
  final def startLocal[T](flow: Flow[(Offset, String, T), Offset, NotUsed],
                          pluginId: String,
                          tag: String,
                          name: String)(implicit as: ActorSystem, T: Typeable[T]): ActorRef = {
    StreamCoordinator.start(initialize(() => Future.successful(())), source(flow, pluginId, tag), name)
  }

  /**
    * Generic tag indexer that iterates over the collection of events selected
    * via the specified tag and apply the argument indexing function.
    * If the event type is not compatible with the events deserialized from the persistence store
    * the events are skipped.
    * @param index    the indexing function
    * @param pluginId the persistence query plugin id
    * @param tag      the tag to use while selecting the events from the store
    * @param name     the name of this indexer
    * @param as       an implicitly available actor system
    * @param T        a Typeable instance for the event type T
    */
  final def startLocal[T](index: T => Future[Unit], pluginId: String, tag: String, name: String)(
      implicit as: ActorSystem,
      T: Typeable[T]): ActorRef =
    startLocal(() => Future.successful(()), index, pluginId, tag, name)
  // $COVERAGE-ON$
}
