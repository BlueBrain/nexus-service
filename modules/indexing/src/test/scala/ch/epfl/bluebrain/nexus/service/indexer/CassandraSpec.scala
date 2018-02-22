package ch.epfl.bluebrain.nexus.service.indexer

import java.io.File

import akka.persistence.cassandra.testkit.CassandraLauncher
import ch.epfl.bluebrain.nexus.service.indexer.persistence._
import org.scalatest.{BeforeAndAfterAll, Suites}

class CassandraSpec
    extends Suites(
      new SequentialTagIndexerSpec,
      new ResumableProjectionSpec,
      new IndexFailuresStorageSpec
    )
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val cassandraDirectory = new File("target/cassandra")
    CassandraLauncher.start(
      cassandraDirectory,
      configResource = CassandraLauncher.DefaultTestConfigResource,
      clean = true,
      port = 0,
      CassandraLauncher.classpathForResources("logback-test.xml")
    )
  }

  override protected def afterAll(): Unit = {
    CassandraLauncher.stop()
    super.afterAll()
  }
}
