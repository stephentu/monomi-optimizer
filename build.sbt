libraryDependencies ++= Seq(
  "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
  "org.specs2" %% "specs2" % "1.8.2" % "test",
  "net.liftweb" %% "lift-json" % "2.4"
)

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"
