import Dependencies._

ThisBuild / scalaVersion := "2.13.8"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.github.hayasshi"

lazy val root = (project in file("."))
  .settings(
    name := "akka-raft",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += Akka.actor,
    libraryDependencies += Akka.testkit % Test
  )
