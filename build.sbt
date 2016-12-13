scalaVersion := "2.11.7"

lazy val root = (project in file(".")).
  settings(
    name := "iodf",
    organization := "com.futurice",
    version := "0.1",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.11.7",
      "org.xerial.larray" % "larray_2.11" % "0.3.4",

      "org.slf4j" % "slf4j-api" % "1.7.21",
      "log4j" % "log4j" % "1.2.17" % Test,
      "org.slf4j" % "slf4j-log4j12" % "1.7.21" % Test,
      "com.futurice" %% "testtoys" % "0.2" % Test
    )
)

