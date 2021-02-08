package xyz.nagendra.kafka.streaming

import com.lightbend.kafka.scala.streams.DefaultSerdes.stringSerde
import com.lightbend.kafka.scala.streams.ImplicitConversions.{consumedFromSerde, producedFromSerde}
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import org.apache.kafka.streams.{KafkaStreams, Topology}

object CapitalizedPipe extends App {

  val inputTopic = "streams-plaintext-input"
  val outputTopic = "streams-pipe-output"

  // 1. Get the streams builder
  val builder = new StreamsBuilderS()

  // 2. Create a source stream from the input topic
  //    The source stream is a `KStreamS` that is generating records from its source kafka topic (inputTopic)
  //    The records are organized as (String, String) key-value pairs
  val source = builder.stream[String, String](inputTopic)

  // 3. Simply pipe the contents from input topic, transform all text to UPPERCASE, and then to the output topic
  source.mapValues(value => value.toUpperCase).to(outputTopic)

  // 4. Examine the topology created above
  val topology: Topology = builder.build()
  println(s"Topology is: ${topology.describe()}")

  // 5. Init the streams client and start it
  val streams = new KafkaStreams(topology, Util.kafkaStreamsProps)
  println("Starting kafka stream ...")
  streams.start()

  // The program will run until it is aborted.
  // Execute a shutdown hook to close the stream before shutting down the app.
  sys.addShutdownHook(() => Util.closeStream(streams))
}
