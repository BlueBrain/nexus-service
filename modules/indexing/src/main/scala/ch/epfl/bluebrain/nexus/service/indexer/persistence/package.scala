package ch.epfl.bluebrain.nexus.service.indexer

import cats.effect.{IO, LiftIO}

import scala.concurrent.Future

package object persistence {

  private[persistence] def liftIO[F[_], A](future: Future[A])(implicit L: LiftIO[F]): F[A] =
    L.liftIO(IO.fromFuture(IO(future)))

}
