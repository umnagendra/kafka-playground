package xyz.nagendra.kafka.streaming

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde

object CapitalizedPipe extends App with TopologyDefinition {

  lazy val appId       = "streams-capitalized-pipe"
  lazy val inputTopic  = "streams-plaintext-input"
  lazy val outputTopic = "streams-pipe-output"

  // Examine the topology created above
  val topology: Topology = createTopology()

  // Init the streams client and start it
  val streams = new KafkaStreams(topology, Util.kafkaStreamsProps(appId))
  println(s"Topology is: ${topology.describe()}")

  override def createTopology() = {
    // 1. Get the streams builder
    val builder = new StreamsBuilder()

    // 2. Create a source stream from the input topic
    //    The source stream is a `KStream` that is generating records from its source kafka topic (inputTopic)
    //    The records are organized as (String, String) key-value pairs
    val source = builder.stream[String, String](inputTopic)

    // 3. Simply pipe the contents from input topic, transform all text to UPPERCASE, and then to the output topic
    source.mapValues(value => value.toUpperCase).to(outputTopic)

    // 4. Build the topology
    builder.build()
  }

  // The program will run until it is aborted.
  // Execute a shutdown hook to close the stream before shutting down the app.
  sys.addShutdownHook(() => Util.closeStream(streams))

  println("Starting kafka stream ...")
  streams.start()
}
