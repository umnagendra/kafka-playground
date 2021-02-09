name := "kafka-playground"
version := "0.1"

libraryDependencies ++= Seq(
  "com.lightbend" % "kafka-streams-scala_2.12" % "0.2.1" % "compile"
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