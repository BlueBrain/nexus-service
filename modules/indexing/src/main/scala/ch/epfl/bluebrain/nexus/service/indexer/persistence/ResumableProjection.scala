package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.actor.ActorSystem
import akka.persistence.query.Offset

import scala.concurrent.Future

/**
  * A ResumableProjection allows storing the current projection progress based on an offset description such that it
  * can be resumed when interrupted (either intentionally or as a consequence for an arbitrary failure).
  *
  * Example use:
  * {{{
  *
  *   implicit val as: ActorSystem = ActorSystem()
  *   val proj = ResumableProjection("default")
  *   proj.fetchLatestOffset // Future[Offset]
  *
  * }}}
  */
trait ResumableProjection {

  /**
    * @return an unique identifier for this projection
    */
  def identifier: String

  /**
    * @return the latest known offset; an inexistent offset is represented by [[akka.persistence.query.NoOffset]]
    */
  def fetchLatestOffset: Future[Offset]

  /**
    * Records the argument offset against this projection progress.
    *
    * @param offset the offset to record
    * @return a future () value upon success or a failure otherwise
    */
  def storeLatestOffset(offset: Offset): Future[Unit]
}

object ResumableProjection {

  private[persistence] def apply(id: String, storage: ProjectionStorage) =
    new ResumableProjection {
      override val identifier: String = id

      override def storeLatestOffset(offset: Offset): Future[Unit] =
        storage.storeOffset(identifier, offset)

      override def fetchLatestOffset: Future[Offset] =
        storage.fetchLatestOffset(identifier)
    }

  /**
    * Constructs a new `ResumableProjection` instance with the specified identifier.  Calls to store or query the
    * current offset are delegated to the underlying
    * [[ch.epfl.bluebrain.nexus.service.indexer.persistence.ProjectionStorage]] extension.
    *
    * @param id an identifier for the projection
    * @param as an implicitly available actor system
    * @return a new `ResumableProjection` instance with the specified identifier
    */
  // $COVERAGE-OFF$
  def apply(id: String)(implicit as: ActorSystem): ResumableProjection =
    apply(id, ProjectionStorage(as))
  // $COVERAGE-ON$
}