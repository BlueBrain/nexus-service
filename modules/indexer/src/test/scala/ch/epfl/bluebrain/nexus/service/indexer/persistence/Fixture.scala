package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.persistence.journal.{Tagged, WriteEventAdapter}
import com.github.ghik.silencer.silent

object Fixture {

  sealed trait Event
  final case object Executed        extends Event
  final case object OtherExecuted   extends Event
  final case object AnotherExecuted extends Event
  final case object RetryExecuted   extends Event
  final case object IgnoreExecuted  extends Event

  sealed trait Cmd
  final case object Execute        extends Cmd
  final case object ExecuteOther   extends Cmd
  final case object ExecuteAnother extends Cmd
  final case object ExecuteRetry   extends Cmd
  final case object ExecuteIgnore  extends Cmd

  sealed trait State
  final case object Perpetual extends State

  sealed trait Rejection
  final case object Reject extends Rejection

  class TaggingAdapter extends WriteEventAdapter {
    override def manifest(event: Any): String = ""
    override def toJournal(event: Any): Any = event match {
      case Executed        => Tagged(event, Set("executed"))
      case OtherExecuted   => Tagged(event, Set("other"))
      case AnotherExecuted => Tagged(event, Set("another"))
      case RetryExecuted   => Tagged(event, Set("retry"))
      case IgnoreExecuted  => Tagged(event, Set("ignore"))

    }
  }

  val initial: State = Perpetual
  @silent
  def next(state: State, event: Event): State = Perpetual
  @silent
  def eval(state: State, cmd: Cmd): Either[Rejection, Event] = cmd match {
    case Execute        => Right(Executed)
    case ExecuteOther   => Right(OtherExecuted)
    case ExecuteAnother => Right(AnotherExecuted)
    case ExecuteRetry   => Right(RetryExecuted)
    case ExecuteIgnore  => Right(IgnoreExecuted)

  }
}
