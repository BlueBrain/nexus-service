package ch.epfl.bluebrain.nexus.service.kafka

package object key {

  /**
    * Implicit class to provide a ''key'' extension method given the presence
    * of a [[Key]] instance.
    */
  implicit class ToKeyOps[A](a: A)(implicit tc: Key[A]) {
    def toKey: String = tc.key(a)
  }
}
