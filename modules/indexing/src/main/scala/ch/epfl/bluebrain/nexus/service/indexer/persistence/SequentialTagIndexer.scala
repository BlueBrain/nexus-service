package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.query.Offset
import cats.effect.Effect
import ch.epfl.bluebrain.nexus.service.indexer.StreamByTag
import ch.epfl.bluebrain.nexus.service.indexer.StreamByTag.{PersistentStreamByTag, VolatileStreamByTag}
import ch.epfl.bluebrain.nexus.service.indexer.persistence.OffsetStorage._
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator
import io.circe.Encoder
import monix.eval.Task
import monix.execution.Scheduler
import shapeless.Typeable

/**
  * Generic tag indexer that uses the specified resumable projection to iterate over the collection of events selected
  * via the specified tag and apply the argument indexing function.  It starts as a singleton actor in a
  * clustered deployment.  If the event type is not compatible with the events deserialized from the persistence store
  * the events are skipped.
  */
object SequentialTagIndexer {

  /**
    * Generic tag indexer that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are NOT persisted once computed the index function.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    * @tparam Event the event type
    */
  final def start[F[_]: Effect, Event: Typeable, MappedEvt, Err](
      config: IndexerConfig[F, Event, MappedEvt, Err, Volatile])(implicit as: ActorSystem, sc: Scheduler): ActorRef = {
    val streamByTag: StreamByTag[F, Offset] = new VolatileStreamByTag(config)
    StreamCoordinator.start(streamByTag.fetchInit, streamByTag.source, config.name)
  }

  /**
    * Generic tag indexer that iterates over the collection of events selected via the specified tag.
    * The offset and the failures are persisted once computed the index function.
    *
    * @param config the index configuration which holds the necessary information to start the tag indexer
    * @tparam Event the event type
    */
  final def start[F[_]: Effect, Event: Typeable, MappedEvt: Encoder, Err](
      config: IndexerConfig[F, Event, MappedEvt, Err, Persist])(implicit failureLog: IndexFailuresLog[F],
                                                                projection: ResumableProjection[F],
                                                                as: ActorSystem,
                                                                sc: Scheduler): ActorRef = {
    val streamByTag: StreamByTag[F, Offset] = new PersistentStreamByTag(config)
    StreamCoordinator.start(streamByTag.fetchInit, streamByTag.source, config.name)
  }

  final def start[Event: Typeable, MappedEvt: Encoder, Err](
      config: IndexerConfig[Task, Event, MappedEvt, Err, Persist])(implicit
                                                                   as: ActorSystem,
                                                                   sc: Scheduler): ActorRef = {
    implicit val projection: ResumableProjection[Task] = ResumableProjection(config.name)
    implicit val failureLog: IndexFailuresLog[Task]    = IndexFailuresLog(config.name)
    start[Task, Event, MappedEvt, Err](config)
  }
  // $COVERAGE-ON$
}
