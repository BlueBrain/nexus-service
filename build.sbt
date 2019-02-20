/*
scalafmt: {
  style = defaultWithAlign
  maxColumn = 150
  align.tokens = [
    { code = "=>", owner = "Case" }
    { code = "?", owner = "Case" }
    { code = "extends", owner = "Defn.(Class|Trait|Object)" }
    { code = "//", owner = ".*" }
    { code = "{", owner = "Template" }
    { code = "}", owner = "Template" }
    { code = ":=", owner = "Term.ApplyInfix" }
    { code = "++=", owner = "Term.ApplyInfix" }
    { code = "+=", owner = "Term.ApplyInfix" }
    { code = "%", owner = "Term.ApplyInfix" }
    { code = "%%", owner = "Term.ApplyInfix" }
    { code = "%%%", owner = "Term.ApplyInfix" }
    { code = "->", owner = "Term.ApplyInfix" }
    { code = "?", owner = "Term.ApplyInfix" }
    { code = "<-", owner = "Enumerator.Generator" }
    { code = "?", owner = "Enumerator.Generator" }
    { code = "=", owner = "(Enumerator.Val|Defn.(Va(l|r)|Def|Type))" }
  ]
}
 */

val akkaVersion                     = "2.5.21"
val akkaHttpVersion                 = "10.1.7"
val akkaHttpCirceVersion            = "1.25.2"
val akkaPersistenceInMemVersion     = "2.5.1.1"
val akkaPersistenceCassandraVersion = "0.92"
val catsVersion                     = "1.4.0"
val catsEffectVersion               = "1.2.0"
val circeVersion                    = "0.11.1"
val commonsVersion                  = "0.10.44"
val journalVersion                  = "3.0.19"
val monixVersion                    = "3.0.0-RC2"
val pureconfigVersion               = "0.10.2"
val scalaTestVersion                = "3.0.5"
val shapelessVersion                = "2.3.3"
val sourcingVersion                 = "0.12.3"

lazy val akkaActor           = "com.typesafe.akka" %% "akka-actor"            % akkaVersion
lazy val akkaCluster         = "com.typesafe.akka" %% "akka-cluster"          % akkaVersion
lazy val akkaClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion
lazy val akkaDistributedData = "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion
lazy val akkaTestKit         = "com.typesafe.akka" %% "akka-testkit"          % akkaVersion
lazy val akkaHttp            = "com.typesafe.akka" %% "akka-http"             % akkaHttpVersion
lazy val akkaHttpTestKit     = "com.typesafe.akka" %% "akka-http-testkit"     % akkaHttpVersion
lazy val akkaStream          = "com.typesafe.akka" %% "akka-stream"           % akkaVersion
lazy val akkaSlf4j           = "com.typesafe.akka" %% "akka-slf4j"            % akkaVersion

lazy val akkaPersistence          = "com.typesafe.akka"   %% "akka-persistence"                    % akkaVersion
lazy val akkaPersistenceQuery     = "com.typesafe.akka"   %% "akka-persistence-query"              % akkaVersion
lazy val akkaPersistenceCassandra = "com.typesafe.akka"   %% "akka-persistence-cassandra"          % akkaPersistenceCassandraVersion
lazy val akkaPersistenceInMem     = "com.github.dnvriend" %% "akka-persistence-inmemory"           % akkaPersistenceInMemVersion
lazy val akkaPersistenceLauncher  = "com.typesafe.akka"   %% "akka-persistence-cassandra-launcher" % akkaPersistenceCassandraVersion

lazy val akkaHttpCirce      = "de.heikoseeberger"       %% "akka-http-circe"      % akkaHttpCirceVersion
lazy val circeCore          = "io.circe"                %% "circe-core"           % circeVersion
lazy val circeParser        = "io.circe"                %% "circe-parser"         % circeVersion
lazy val circeGenericExtras = "io.circe"                %% "circe-generic-extras" % circeVersion
lazy val catsCore           = "org.typelevel"           %% "cats-core"            % catsVersion
lazy val catsEffect         = "org.typelevel"           %% "cats-effect"          % catsEffectVersion
lazy val commonsTest        = "ch.epfl.bluebrain.nexus" %% "commons-test"         % commonsVersion
lazy val commonsTypes       = "ch.epfl.bluebrain.nexus" %% "commons-types"        % commonsVersion
lazy val commonsHttp        = "ch.epfl.bluebrain.nexus" %% "commons-http"         % commonsVersion
lazy val journal            = "io.verizon.journal"      %% "core"                 % journalVersion
lazy val monixEval          = "io.monix"                %% "monix-eval"           % monixVersion
lazy val pureconfig         = "com.github.pureconfig"   %% "pureconfig"           % pureconfigVersion
lazy val shapeless          = "com.chuusai"             %% "shapeless"            % shapelessVersion
lazy val sourcingAkka       = "ch.epfl.bluebrain.nexus" %% "sourcing-akka"        % sourcingVersion

lazy val kamonCore       = "io.kamon" %% "kamon-core"            % "1.1.5"
lazy val kamonPrometheus = "io.kamon" %% "kamon-prometheus"      % "1.1.1"
lazy val kamonJaeger     = "io.kamon" %% "kamon-jaeger"          % "1.0.2"
lazy val kamonLogback    = "io.kamon" %% "kamon-logback"         % "1.0.5"
lazy val kamonMetrics    = "io.kamon" %% "kamon-system-metrics"  % "1.0.1"
lazy val kamonAkka       = "io.kamon" %% "kamon-akka-2.5"        % "1.1.3"
lazy val kamonAkkaHttp   = "io.kamon" %% "kamon-akka-http-2.5"   % "1.1.1"
lazy val kamonAkkaRemote = "io.kamon" %% "kamon-akka-remote-2.5" % "1.1.0"

lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion

lazy val http = project
  .in(file("modules/http"))
  .settings(
    name       := "service-http",
    moduleName := "service-http",
    libraryDependencies ++= Seq(
      akkaHttp,
      akkaHttpCirce,
      catsEffect,
      circeCore,
      circeParser,
      commonsHttp,
      akkaTestKit        % Test,
      commonsTest        % Test,
      akkaHttpTestKit    % Test,
      circeGenericExtras % Test,
      circeParser        % Test,
      scalaTest          % Test
    )
  )

lazy val test = project
  .in(file("modules/test"))
  .settings(
    name                := "service-test",
    moduleName          := "service-test",
    coverageEnabled     := false,
    libraryDependencies ++= Seq(akkaClusterSharding, akkaTestKit, scalaTest)
  )

lazy val indexing = project
  .in(file("modules/indexing"))
  .dependsOn(test)
  .settings(
    name       := "service-indexing",
    moduleName := "service-indexing",
    libraryDependencies ++= Seq(
      akkaActor,
      akkaCluster,
      akkaDistributedData,
      akkaPersistenceCassandra,
      circeCore,
      circeParser,
      commonsTypes,
      journal,
      monixEval,
      pureconfig,
      shapeless,
      sourcingAkka,
      akkaPersistenceLauncher % Test,
      akkaTestKit             % Test,
      akkaHttpTestKit         % Test,
      akkaSlf4j               % Test,
      circeGenericExtras      % Test,
      commonsTest             % Test,
      pureconfig              % Test,
      scalaTest               % Test,
    )
  )

lazy val serialization = project
  .in(file("modules/serialization"))
  .settings(
    name       := "service-serialization",
    moduleName := "service-serialization",
    libraryDependencies ++= Seq(
      akkaActor,
      circeCore,
      circeParser,
      shapeless,
      circeGenericExtras % Test,
      scalaTest          % Test
    )
  )

lazy val kamon = project
  .in(file("modules/kamon"))
  .settings(
    name       := "service-kamon",
    moduleName := "service-kamon",
    libraryDependencies ++= Seq(
      kamonCore,
      kamonPrometheus,
      kamonJaeger,
      kamonLogback,
      kamonMetrics,
      kamonAkka % Runtime,
      kamonAkkaHttp,
      kamonAkkaRemote % Runtime,
      akkaHttpTestKit % Test,
      akkaTestKit     % Test,
      akkaSlf4j       % Test,
      scalaTest       % Test
    )
  )

lazy val root = project
  .in(file("."))
  .settings(noPublish)
  .settings(
    name       := "service",
    moduleName := "service"
  )
  .aggregate(http, test, indexing, serialization, kamon)

/* ********************************************************
 ******************** Grouped Settings ********************
 **********************************************************/

lazy val noPublish = Seq(
  publishLocal    := {},
  publish         := {},
  publishArtifact := false
)

inThisBuild(
  List(
    homepage := Some(url("https://github.com/BlueBrain/nexus-service")),
    licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    scmInfo  := Some(ScmInfo(url("https://github.com/BlueBrain/nexus-service"), "scm:git:git@github.com:BlueBrain/nexus-service.git")),
    developers := List(
      Developer("bogdanromanx", "Bogdan Roman", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
      Developer("hygt", "Henry Genet", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
      Developer("umbreak", "Didac Montero Mendez", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
      Developer("wwajerowicz", "Wojtek Wajerowicz", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/"))
    ),
    // These are the sbt-release-early settings to configure
    releaseEarlyWith              := BintrayPublisher,
    releaseEarlyNoGpg             := true,
    releaseEarlyEnableSyncToMaven := false
  ))

addCommandAlias("review", ";clean;scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck;coverage;scapegoat;test;coverageReport;coverageAggregate")
