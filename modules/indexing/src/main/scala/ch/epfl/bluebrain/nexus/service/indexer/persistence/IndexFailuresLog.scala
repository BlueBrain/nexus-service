package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.actor.ActorSystem
import akka.persistence.query.Offset
import akka.stream.scaladsl.Source
import io.circe.{Decoder, Encoder}

import scala.concurrent.Future

trait IndexFailuresLog {

  /**
    * @return an unique identifier for this failures log
    */
  def identifier: String

  /**
    * Records the failed event against this failures log.
    *
    * @param persistenceId the persistenceId to record
    * @param offset        the offset to record
    * @param event         the event to record
    * @tparam T the generic type of the ''event''
    * @return a future of [[Unit]] upon success or a failure otherwise
    */
  def storeEvent[T](persistenceId: String, offset: Offset, event: T)(implicit E: Encoder[T]): Future[Unit]

  /**
    * Retrieve the events for this failures log.
    *
    * @tparam T the generic type of the returned ''event''s
    */
  def fetchEvents[T](implicit D: Decoder[T]): Source[T, _]
}

object IndexFailuresLog {
  private[persistence] def apply(id: String, storage: IndexFailuresStorage): IndexFailuresLog = new IndexFailuresLog {

    override val identifier: String = id

    override def storeEvent[T](persistenceId: String, offset: Offset, event: T)(implicit E: Encoder[T]): Future[Unit] =
      storage.storeEvent(identifier, persistenceId, offset, event)

    override def fetchEvents[T](implicit D: Decoder[T]): Source[T, _] = storage.fetchEvents(identifier)

  }

  /**
    * Constructs a new `IndexFailuresLog` instance with the specified identifier.
    * Calls to store or query the current event are delegated to the underlying
    * [[IndexFailuresStorage]] extension.
    *
    * @param id an identifier for the failures log
    * @param as an implicitly available actor system
    */
  final def apply(id: String)(implicit as: ActorSystem): IndexFailuresLog =
    apply(id, IndexFailuresStorage(as))
}
