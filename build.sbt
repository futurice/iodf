scalaVersion := "2.11.7"

lazy val root = (project in file(".")).
  settings(
    name := "iodf",
    organization := "com.futurice",
    version := "0.1",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.11.7",
      "org.xerial.larray" % "larray_2.11" % "0.3.4",
      "com.futurice" %% "testtoys" % "0.2" % Test
    )
)

