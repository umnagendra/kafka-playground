name := "kafka-playground"
version := "0.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams-scala_2.12" % "2.7.0" % "compile",
  "org.scalatest"   %% "scalatest"                % "3.2.5" % Test,
  "org.apache.kafka" % "kafka-streams-test-utils" % "2.7.0" % Test
)

////////////////////////////////
// Example of a custom SBT task
// Execute with `sbt exampleTask`
////////////////////////////////
lazy val exampleTask = taskKey[Unit]("An example task that will return no value")

exampleTask := {
  val s: TaskStreams = streams.value
  s.log.info("Inside example task ...")
  s.log.info(s"Project Details: ${name}:${version} built using Scala ${scalaVersion} (${scalaHome})")
}
