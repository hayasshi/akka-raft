import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.12"

  object Akka {
    val version = "2.6.19"

    lazy val actor = "com.typesafe.akka" %% "akka-actor" % version
    lazy val testkit = "com.typesafe.akka" %% "akka-testkit" % version
  }
}
