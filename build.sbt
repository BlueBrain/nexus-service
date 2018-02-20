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

val akkaVersion          = "2.5.9"
val akkaHttpVersion      = "10.0.11"
val akkaHttpCirceVersion = "1.19.0"
val catsVersion          = "1.0.1"
val circeVersion         = "0.9.1"
val commonsVersion       = "0.7.6"
val journalVersion       = "3.0.19"
val scalaTestVersion     = "3.0.5"

lazy val akkaTestKit        = "com.typesafe.akka"       %% "akka-testkit"         % akkaVersion
lazy val akkaHttp           = "com.typesafe.akka"       %% "akka-http"            % akkaHttpVersion
lazy val akkaHttpTestKit    = "com.typesafe.akka"       %% "akka-http-testkit"    % akkaHttpVersion
lazy val akkaHttpCirce      = "de.heikoseeberger"       %% "akka-http-circe"      % akkaHttpCirceVersion
lazy val catsCore           = "org.typelevel"           %% "cats-core"            % catsVersion
lazy val circeCore          = "io.circe"                %% "circe-core"           % circeVersion
lazy val circeParser        = "io.circe"                %% "circe-parser"         % circeVersion
lazy val circeGenericExtras = "io.circe"                %% "circe-generic-extras" % circeVersion
lazy val commonsTest        = "ch.epfl.bluebrain.nexus" %% "commons-test"         % commonsVersion
lazy val journal            = "io.verizon.journal"      %% "core"                 % journalVersion
lazy val scalaTest          = "org.scalatest"           %% "scalatest"            % scalaTestVersion

lazy val serviceHttp = project
  .in(file("modules/http"))
  .settings(
    name       := "service-http",
    moduleName := "service-http",
    libraryDependencies ++= Seq(
      akkaHttp,
      akkaHttpCirce,
      catsCore,
      circeCore,
      circeParser,
      journal,
      akkaTestKit        % Test,
      akkaHttpTestKit    % Test,
      circeGenericExtras % Test,
      commonsTest        % Test,
      scalaTest          % Test
    )
  )

lazy val root = project
  .in(file("."))
  .settings(noPublish)
  .settings(
    name       := "service",
    moduleName := "service",
  )
  .aggregate(serviceHttp)

/* ********************************************************
 ******************** Grouped Settings ********************
 **********************************************************/

lazy val noPublish = Seq(
  publishLocal    := {},
  publish         := {},
  publishArtifact := false,
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
      Developer("wwajerowicz", "Wojtek Wajerowicz", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
    ),
    // These are the sbt-release-early settings to configure
    releaseEarlyWith              := BintrayPublisher,
    releaseEarlyNoGpg             := true,
    releaseEarlyEnableSyncToMaven := false,
  ))

addCommandAlias("review", ";clean;scalafmtSbtCheck;coverage;scapegoat;test;coverageReport;coverageAggregate")
