ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "4.10.0"


lazy val root = (project in file("."))
  .settings(
    name := "SimpleSearchEngine"
  )
