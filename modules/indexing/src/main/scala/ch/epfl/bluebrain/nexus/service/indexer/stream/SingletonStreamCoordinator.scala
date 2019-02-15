package ch.epfl.bluebrain.nexus.service.indexer.stream

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.stream.scaladsl.Source
import cats.effect.Effect
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator.Stop
import monix.execution.Scheduler
import shapeless.Typeable

object SingletonStreamCoordinator {

  /**
    * Builds a [[Props]] for a [[SingletonStreamCoordinator]] with it cluster singleton configuration.
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  // $COVERAGE-OFF$
  final def props[F[_]: Effect, A: Typeable](init: F[A], source: A => Source[A, _])(implicit as: ActorSystem,
                                                                                    sc: Scheduler): Props =
    ClusterSingletonManager.props(Props(new StreamCoordinator(init, source)),
                                  terminationMessage = Stop,
                                  settings = ClusterSingletonManagerSettings(as))

  /**
    * Builds a cluster singleton actor of type [[SingletonStreamCoordinator]].
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  final def start[F[_]: Effect, A: Typeable](init: F[A], source: A => Source[A, _], name: String)(
      implicit as: ActorSystem,
      sc: Scheduler): ActorRef =
    as.actorOf(props(init, source), name)
  // $COVERAGE-ON$
}
