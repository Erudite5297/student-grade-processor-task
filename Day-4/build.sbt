ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day-4-rdd-practice",
    version := "1.0"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1"
)

Compile / run / fork := true

Compile / run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)
