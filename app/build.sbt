name := "web-log-analyzer"
organization in ThisBuild := "com.ansel"
scalaVersion in ThisBuild := "2.11.12"
val sparkVersion = "2.4.0"

lazy val global = project
  .in(file("."))
  .settings(name := "web-log-analyzer")
  .settings(commonSettings: _*)
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    sessionizer,
    analyzer
  )

lazy val sessionizer = project
  .settings(name := "sessionizer")
  .settings(commonSettings: _*)
  .disablePlugins(AssemblyPlugin)

lazy val analyzer = project
  .settings(name := "analyzer")
  .settings(commonSettings: _*)
  .dependsOn(sessionizer)

val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

    // test dependencies
    "org.apache.spark" %% "spark-core" % sparkVersion % "test",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "test",
    "org.scalactic" %% "scalactic" % "3.0.1" % "test",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )
)
