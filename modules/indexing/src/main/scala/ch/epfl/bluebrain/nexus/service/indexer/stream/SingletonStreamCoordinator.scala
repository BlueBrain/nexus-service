package ch.epfl.bluebrain.nexus.service.indexer.stream

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.stream.scaladsl.Source
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator.Stop
import shapeless.Typeable

import scala.concurrent.Future

object SingletonStreamCoordinator {

  /**
    * Builds a [[Props]] for a [[SingletonStreamCoordinator]] with it cluster singleton configuration.
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  // $COVERAGE-OFF$
  final def props[A: Typeable, E](init: () => Future[A], source: A => Source[E, _])(implicit as: ActorSystem): Props =
    ClusterSingletonManager.props(Props(new StreamCoordinator(init, source)),
                                  terminationMessage = Stop,
                                  settings = ClusterSingletonManagerSettings(as))

  /**
    * Builds a cluster singleton actor of type [[SingletonStreamCoordinator]].
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  final def start[A: Typeable, E](init: () => Future[A], source: A => Source[E, _], name: String)(
      implicit as: ActorSystem): ActorRef =
    as.actorOf(props(init, source), name)
  // $COVERAGE-ON$
}
