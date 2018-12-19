package ch.epfl.bluebrain.nexus.service.indexer.cache

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.ddata.LWWRegister.Clock
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{DistributedData, LWWMap, LWWMapKey}
import akka.pattern.ask
import akka.util.Timeout
import cats.{Functor, Monad}
import cats.effect.{Async, IO, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreError._
import ch.epfl.bluebrain.nexus.sourcing.akka.RetryStrategy

import scala.concurrent.duration.FiniteDuration

/**
  * An arbitrary key value store.
  *
  * @tparam F the effect type
  * @tparam K the key type
  * @tparam V the value type
  */
trait KeyValueStore[F[_], K, V] {

  /**
    * Adds the (key, value) to the store, replacing the current value if the key already exists.
    *
    * @param key   the key under which the value is stored
    * @param value the value stored
    */
  def put(key: K, value: V): F[Unit]

  /**
    * Adds the (key, value) to the store only if the key does not exists.
    *
    * @param key   the key under which the value is stored
    * @param value the value stored
    * @return true if the value was added, false otherwise. The response is wrapped on the effect type ''F[_]''
    */
  def putIfAbsent(key: K, value: V)(implicit F: Monad[F]): F[Boolean] =
    get(key).flatMap {
      case Some(_) => F.pure(false)
      case _       => put(key, value).map(_ => true)
    }

  /**
    * If the value for the specified key is present, attempts to compute a new mapping given the key and its current mapped value.
    *
    * @param key the key under which the value is stored
    * @param f   the function to compute a value
    * @return None wrapped on the effect type ''F[_]'' if the value does not exist for the given key.
    *         Some(value) wrapped on the effect type ''F[_]''
    *         where value is the result of computing the provided f function on the current value of the provided key
    */
  def computeIfPresent(key: K, f: V => V)(implicit F: Monad[F]): F[Option[V]] =
    get(key).flatMap {
      case Some(value) =>
        val computedValue = f(value)
        put(key, computedValue).map(_ => Some(computedValue))
      case other => F.pure(other)
    }

  /**
    * @return all the entries in the store
    */
  def entries(): F[Map[K, V]]

  /**
    * @return a set of all the values in the store
    */
  def values()(implicit F: Functor[F]): F[Set[V]] =
    entries().map(_.values.toSet)

  /**
    * @param key the key
    * @return an optional value for the provided key
    */
  def get(key: K)(implicit F: Functor[F]): F[Option[V]] =
    entries().map(_.get(key))

  /**
    * Finds the first (key, value) pair that satisfies the predicate.
    *
    * @param f the predicate to the satisfied
    * @return the first (key, value) pair that satisfies the predicate or None if none are found
    */
  def find(f: (K, V) => Boolean)(implicit F: Functor[F]): F[Option[(K, V)]] =
    entries().map(_.find { case (k, v) => f(k, v) })

  /**
    * Finds the first value in the store that satisfies the predicate.
    *
    * @param f the predicate to the satisfied
    * @return the first value that satisfies the predicate or None if none are found
    */
  def findValue(f: V => Boolean)(implicit F: Functor[F]): F[Option[V]] =
    entries().map(_.find { case (_, v) => f(v) }.map { case (_, v) => v })
}

object KeyValueStore {

  /**
    * Constructs a key value store backed by Akka Distributed Data with WriteAll and ReadLocal consistency
    * configuration. The store is backed by a LWWMap.
    *
    * @param id     the ddata key
    * @param clock  a clock function that determines the next timestamp for a provided value
    * @param as     the implicitly underlying actor system
    * @param config the key value store configuration
    * @tparam F the effect type
    * @tparam K the key type
    * @tparam V the value type
    */
  final def distributed[F[_]: Async: Timer, K, V](id: String, clock: (Long, V) => Long)(
      implicit as: ActorSystem,
      config: KeyValueStoreConfig): KeyValueStore[F, K, V] =
    new DDataKeyValueStore(id, clock, config.askTimeout, config.consistencyTimeout, config.retryStrategy)

  private class DDataKeyValueStore[F[_]: Async, K, V](
      id: String,
      clock: (Long, V) => Long,
      askTimeout: FiniteDuration,
      consistencyTimeout: FiniteDuration,
      retryStrategy: RetryStrategy[F]
  )(implicit as: ActorSystem)
      extends KeyValueStore[F, K, V] {

    private implicit val node: Cluster           = Cluster(as)
    private implicit val registerClock: Clock[V] = (currentTimestamp: Long, value: V) => clock(currentTimestamp, value)
    private implicit val timeout: Timeout        = Timeout(askTimeout)

    private val F                       = implicitly[Async[F]]
    private val replicator              = DistributedData(as).replicator
    private val mapKey                  = LWWMapKey[K, V](id)
    private val consistencyTimeoutError = ReadWriteConsistencyTimeout(consistencyTimeout)

    override def put(key: K, value: V): F[Unit] = {
      val msg    = Update(mapKey, LWWMap.empty[K, V], WriteAll(consistencyTimeout))(_.put(key, value))
      val future = IO(replicator ? msg)
      val fa     = IO.fromFuture(future).to[F]
      val mappedFA = fa.flatMap[Unit] {
        case _: UpdateSuccess[_] => F.unit
        // $COVERAGE-OFF$
        case _: UpdateTimeout[_] => F.raiseError(consistencyTimeoutError)
        case _: UpdateFailure[_] => F.raiseError(DistributedDataError("Failed to distribute write"))
        // $COVERAGE-ON$
      }
      retryStrategy(mappedFA)
    }

    override def entries(): F[Map[K, V]] = {
      val msg    = Get(mapKey, ReadLocal)
      val future = IO(replicator ? msg)
      val fa     = IO.fromFuture(future).to[F]
      val mappedFA = fa.flatMap[Map[K, V]] {
        case g @ GetSuccess(`mapKey`, _) => F.pure(g.get(mapKey).entries)
        case _: NotFound[_]              => F.pure(Map.empty)
        // $COVERAGE-OFF$
        case _: GetFailure[_] => F.raiseError(consistencyTimeoutError)
        // $COVERAGE-ON$
      }
      retryStrategy(mappedFA)
    }
  }
}
