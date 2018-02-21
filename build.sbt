scalaVersion := "2.12.2"

crossScalaVersions := Seq("2.10.6", "2.11.7")

lazy val iodf = (project in file(".")).
  settings(
    name := "iodf",
    organization := "com.futurice",
    version := "0.1",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.12.2",
      "org.xerial.larray" %% "larray" % "0.4.0",

      "org.slf4j" % "slf4j-api" % "1.7.21",
      "ch.qos.logback" %  "logback-classic" % "1.1.7" % Test,
      "com.futurice" % "testtoys_2.12" % "0.2" % Test,
      "org.scalatest" %% "scalatest" % "3.0.4" % Test,
      "org.scalacheck" %% "scalacheck" % "1.13.4" % Test
    )
)

