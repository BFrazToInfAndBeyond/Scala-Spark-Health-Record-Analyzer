name := "vital-signs"

version := "1.0"

scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
libraryDependencies += "net.liftweb" %% "lift-json" % "2.5.1"
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"