package ch.epfl.bluebrain.nexus.service.indexer.cache

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator.Changed
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreSubscriber.KeyValeStoreChange._
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreSubscriber._

trait OnKeyValueStoreChange[K, V] {

  /**
    * Method that gets triggered when a change to key value store occurs.
    *
    * @param value the changes made
    */
  def apply(value: KeyValeStoreChanges[K, V]): Unit
}

object OnKeyValueStoreChange {
  def noEffect[K, V]: OnKeyValueStoreChange[K, V] = _ => ()
}

/**
  * A subscriber actor that receives messages from the key value store whenever a change occurs.
  *
  * @param onChange the method that gets triggered when a change to key value store occurs
  * @tparam K the key type
  * @tparam V the value type
  */
class KeyValueStoreSubscriber[K, V] private (onChange: OnKeyValueStoreChange[K, V]) extends Actor with ActorLogging {

  private val key      = LWWMapKey[K, V](self.path.name)
  private var previous = Map.empty[K, V]

  private def diff(recent: Map[K, V]): KeyValeStoreChanges[K, V] = {
    val added   = (recent -- previous.keySet).map { case (k, v) => ValueAdded(k, v) }.toSet
    val removed = (previous -- recent.keySet).map { case (k, v) => ValueRemoved(k, v) }.toSet

    val modified = (recent -- added.map(_.key)).foldLeft(Set.empty[KeyValeStoreChange]) {
      case (acc, (k, v)) =>
        previous.get(k).filter(_ == v) match {
          case None => acc + ValueModified(k, v)
          case _    => acc
        }
    }
    KeyValeStoreChanges(added ++ modified ++ removed)
  }

  override def receive: Receive = {
    case c @ Changed(`key`) =>
      val recent  = c.get(key).entries
      val changes = diff(recent)
      if (changes.values.nonEmpty) onChange(changes)
      previous = recent
      log.debug("Received a Changed message from the key value store. Values changed: '{}'", changes)

    case other =>
      log.error("Skipping received a message different from Changed. Message: '{}'", other)

  }
}

object KeyValueStoreSubscriber {

  /**
    * Enumeration of types related to changes to the key value store.
    */
  sealed trait KeyValeStoreChange extends Product with Serializable
  object KeyValeStoreChange {

    /**
      * Signals that an element has been added to the key value store.
      *
      * @param key   the key
      * @param value the value
      */
    final case class ValueAdded[K, V](key: K, value: V) extends KeyValeStoreChange

    /**
      * Signals that an already existing element has been updated from the key value store.
      *
      * @param key   the key
      * @param value the value
      */
    final case class ValueModified[K, V](key: K, value: V) extends KeyValeStoreChange

    /**
      * Signals that an already existing element has been removed from the key value store.
      *
      * @param key   the key
      * @param value the value
      */
    final case class ValueRemoved[K, V](key: K, value: V) extends KeyValeStoreChange
  }

  /**
    * The set of changes that have occurred on the primary store
    *
    * @param values the set of changes
    */
  final case class KeyValeStoreChanges[K, V](values: Set[KeyValeStoreChange])

  /**
    * Constructs the [[KeyValueStoreSubscriber]] actor.
    *
    * @param id       the ddata key
    * @param onChange the method that gets triggered whenever a change to the key value store occurs
    * @tparam K the key type
    * @tparam V the value type
    * @return an [[ActorRef]] of the [[KeyValueStoreSubscriber]] actor
    */
  final def apply[K, V](id: String, onChange: OnKeyValueStoreChange[K, V])(implicit as: ActorSystem): ActorRef =
    as.actorOf(Props(new KeyValueStoreSubscriber(onChange)), id)
}
