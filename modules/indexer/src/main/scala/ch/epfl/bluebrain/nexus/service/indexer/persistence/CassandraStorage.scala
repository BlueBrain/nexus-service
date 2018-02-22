package ch.epfl.bluebrain.nexus.service.indexer.persistence

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.persistence.cassandra.CassandraPluginConfig
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import com.datastax.driver.core.Session
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

trait CassandraStorage {

  /**
    * Initialize Cassandra session and create keyspace and table if not exists.
    *
    * @param name        the prefix for the config property where to find the table name
    * @param tableSchema the schema definition when creating the table
    * @param system the actor system
    * @return a [[Tuple3]] with the session, the keyspace and the table name
    */
  def createSession(name: String,
                    tableSchema: String,
                    system: ExtendedActorSystem): (CassandraSession, String, String) = {
    implicit val ec   = system.dispatcher
    val journalConfig = lookupConfig(system)
    val table         = journalConfig.getString(s"$name-table")
    val config        = new CassandraPluginConfig(system, journalConfig)
    val log           = Logging(system, s"${name.capitalize}Storage")

    val session = new CassandraSession(
      system,
      config.sessionProvider,
      config.sessionSettings,
      ec,
      log,
      metricsCategory = s"$name-storage",
      init = session => executeCreateKeyspaceAndTable(session, config, table, tableSchema)
    )
    (session, config.keyspace, table)
  }

  private def createKeyspace(config: CassandraPluginConfig) = s"""
      CREATE KEYSPACE IF NOT EXISTS ${config.keyspace}
      WITH REPLICATION = { 'class' : ${config.replicationStrategy} }
    """

  private def createTable(config: CassandraPluginConfig, name: String, tableSchema: String) = s"""
      CREATE TABLE IF NOT EXISTS ${config.keyspace}.$name (
        $tableSchema)
     """

  private def executeCreateKeyspaceAndTable(session: Session,
                                            config: CassandraPluginConfig,
                                            tableName: String,
                                            tableSchema: String)(implicit ec: ExecutionContext): Future[Done] = {
    import akka.persistence.cassandra.listenableFutureToFuture
    val keyspace: Future[Done] =
      if (config.keyspaceAutoCreate) session.executeAsync(createKeyspace(config)).map(_ => Done)
      else Future.successful(Done)

    if (config.tablesAutoCreate) {
      for {
        _    <- keyspace
        done <- session.executeAsync(createTable(config, tableName, tableSchema)).map(_ => Done)
      } yield done
    } else keyspace
  }

  private def lookupConfig(system: ExtendedActorSystem): Config =
    system.settings.config.getConfig("cassandra-journal")
}
