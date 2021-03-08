package xyz.nagendra.kafka.streaming

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes.{longSerde, stringSerde}
import org.apache.kafka.streams.{KafkaStreams, Topology}

object WordCount extends App with TopologyDefinition {

  lazy val appId       = "streams-word-count"
  lazy val inputTopic  = "streams-plaintext-input"
  lazy val outputTopic = "streams-word-count"
  lazy val countsStore = "counts-store"

  // Examine the topology created above
  val topology: Topology = createTopology()

  // Init the streams client and start it
  val streams = new KafkaStreams(topology, Util.kafkaStreamsProps(appId))
  println(s"Topology is: ${topology.describe()}")

  override def createTopology() = {
    // 1. Get the streams builder
    val builder = new StreamsBuilder()

    // 2. Create a source stream from the input topic
    //    The source stream is a `KStreamS` that is generating records from its source kafka topic (inputTopic)
    //    The records are organized as (String, String) key-value pairs
    val source = builder.stream[String, String](inputTopic)

    /**
     * NOTES on Concurrency
     * --------------------
     * Processing a partition will always be done by a single "stream task thread" per topic partition
     * (unless the property 'num.stream.threads' is overridden) which ensures we are not running into
     * concurrency issues and ensures all events in a topic partition are processed in order.
     *
     * Ref. https://stackoverflow.com/questions/39985048/kafka-streaming-concurrency
     */
    source
      // 3.1 Lowercase each event entirely
      .mapValues(value => value.toLowerCase)
      // 3.2 Extract individual words from each event from the source topic
      .flatMapValues(value => value.split("\\W+"))
      // 3.3 Group by words
      .groupBy((_, value) => value)
      // 3.4 Count and store the result into a KeyValueStore named "counts-store" as a KTable
      .count()(Materialized.as(countsStore))
      // 3.5 From the counts-store KTable, start a stream into the output topic
      .toStream
      .to(outputTopic)

    // 4. Build the topology
    builder.build()
  }

  // The program will run until it is aborted.
  // Execute a shutdown hook to close the stream before shutting down the app.
  sys.addShutdownHook(Util.closeStream(streams))

  println("Starting kafka stream ...")
  streams.start()
}
