package xyz.nagendra.kafka.streaming

import com.lightbend.kafka.scala.streams.DefaultSerdes.{longSerde, stringSerde}
import com.lightbend.kafka.scala.streams.ImplicitConversions.{consumedFromSerde, producedFromSerde, serializedFromSerde}
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, Topology}

object WordCount extends App {

  val appId = "streams-word-count"
  val inputTopic = "streams-plaintext-input"
  val outputTopic = "streams-word-count"
  val countsStore = "counts-store"

  // 1. Get the streams builder
  val builder = new StreamsBuilderS()

  // 2. Create a source stream from the input topic
  //    The source stream is a `KStreamS` that is generating records from its source kafka topic (inputTopic)
  //    The records are organized as (String, String) key-value pairs
  val source = builder.stream[String, String](inputTopic)

  // 3.1 Lowercase each event entirely
  source.mapValues(value => value.toLowerCase)

  // 3.2 Extract individual words from each event from the source topic
    .flatMapValues(value => value.split("\\W+"))

  // 3.2 Group by words
    .groupBy((_, value) => value)

  // 3.3 Count and store the result into a KeyValueStore named "counts-store" as a KTableS
    .count(countsStore, Some(Serdes.String()))

  // 3.4 From the counts-store KTableS, start a stream into the output topic
    .toStream.to(outputTopic)


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