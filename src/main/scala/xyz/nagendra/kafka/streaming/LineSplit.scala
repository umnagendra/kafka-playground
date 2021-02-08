package xyz.nagendra.kafka.streaming

import com.lightbend.kafka.scala.streams.DefaultSerdes.stringSerde
import com.lightbend.kafka.scala.streams.ImplicitConversions.{consumedFromSerde, producedFromSerde}
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import org.apache.kafka.streams.{KafkaStreams, Topology}

object LineSplit extends App {

  val appId = "streams-line-split"
  val inputTopic = "streams-plaintext-input"
  val outputTopic = "streams-words"

  // 1. Get the streams builder
  val builder = new StreamsBuilderS()

  // 2. Create a source stream from the input topic
  //    The source stream is a `KStreamS` that is generating records from its source kafka topic (inputTopic)
  //    The records are organized as (String, String) key-value pairs
  val source = builder.stream[String, String](inputTopic)

  // 3. Extract individual words from each event from the source topic and put them into the output topic
  source.flatMapValues(value => value.split("\\W+")).to(outputTopic)

  // 4. Examine the topology created above
  val topology: Topology = builder.build()
  println(s"Topology is: ${topology.describe()}")

  // 5. Init the streams client and start it
  val streams = new KafkaStreams(topology, Util.kafkaStreamsProps(appId))

  // The program will run until it is aborted.
  // Execute a shutdown hook to close the stream before shutting down the app.
  sys.addShutdownHook(Util.closeStream(streams))

  println("Starting kafka stream ...")
  streams.start()
}
