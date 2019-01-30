package ch.epfl.bluebrain.nexus.service.indexer.stream

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Status}
import akka.pattern.pipe
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator._
import monix.eval.Task
import monix.execution.Scheduler
import shapeless.Typeable

import scala.concurrent.Future

/**
  * Actor implementation that builds and manages a stream ([[RunnableGraph]]).
  * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
  * @param source an initialization function that produces a stream from an initial start value
  */
class StreamCoordinator[A: Typeable, E](init: Task[A], source: A => Source[E, _])(implicit sc: Scheduler)
    extends Actor
    with ActorLogging {

  private val A                              = implicitly[Typeable[A]]
  private implicit val as: ActorSystem       = context.system
  private implicit val mt: ActorMaterializer = ActorMaterializer()

  private def initialize(): Unit = {
    val logError: PartialFunction[Throwable, Task[Unit]] = {
      case err =>
        log.error(err, "Failed on initialize function with error '{}'", err.getMessage)
        Task.raiseError(err)
    }
    val _ = init.map(Start).onErrorRecoverWith(logError).runToFuture pipeTo self
  }

  override def preStart(): Unit = {
    super.preStart()
    initialize()
  }

  private def buildStream(a: A): RunnableGraph[(UniqueKillSwitch, Future[Done])] = {
    source(a)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
  }

  override def receive: Receive = {
    case Start(any) =>
      A.cast(any) match {
        case Some(a) =>
          log.info(
            "Received initial start value of type '{}', with value '{}' running the indexing function across the element stream",
            A.describe,
            a)
          val (killSwitch, doneFuture) = buildStream(a).run()
          doneFuture pipeTo self
          context.become(running(killSwitch))
        // $COVERAGE-OFF$
        case _ =>
          log.error("Received unknown initial start value '{}', expecting type '{}', stopping", any, A.describe)
          context.stop(self)
        // $COVERAGE-ON$
      }
    // $COVERAGE-OFF$
    case Stop =>
      log.info("Received stop signal while waiting for a start value, stopping")
      context.stop(self)
    // $COVERAGE-ON$
  }

  private def running(killSwitch: UniqueKillSwitch): Receive = {
    case Done =>
      log.error("Stream finished unexpectedly, restarting")
      killSwitch.shutdown()
      initialize()
      context.become(receive)
    // $COVERAGE-OFF$
    case Status.Failure(th) =>
      log.error(th, "Stream finished unexpectedly with an error")
      killSwitch.shutdown()
      initialize()
      context.become(receive)
    // $COVERAGE-ON$
    case Stop =>
      log.info("Received stop signal, stopping stream")
      killSwitch.shutdown()
      context.become(stopping)
  }

  private def stopping: Receive = {
    case Done =>
      log.info("Stream finished, stopping")
      context.stop(self)
    // $COVERAGE-OFF$
    case Status.Failure(th) =>
      log.error("Stream finished with an error", th)
      context.stop(self)
    // $COVERAGE-ON$
  }
}

object StreamCoordinator {
  private[service] final case class Start(any: Any)
  final case object Stop

  /**
    * Builds a [[Props]] for a [[StreamCoordinator]] with its configuration.
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  // $COVERAGE-OFF$
  final def props[A: Typeable, E](init: Task[A], source: A => Source[E, _])(implicit sc: Scheduler): Props =
    Props(new StreamCoordinator(init, source))

  /**
    * Builds a singleton actor of type [[StreamCoordinator]].
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  final def start[A: Typeable, E](init: Task[A], source: A => Source[E, _], name: String)(implicit as: ActorSystem,
                                                                                          sc: Scheduler): ActorRef =
    as.actorOf(props(init, source), name)
  // $COVERAGE-ON$
}
