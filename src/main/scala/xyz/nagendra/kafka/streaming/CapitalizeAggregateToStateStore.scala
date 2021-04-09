package xyz.nagendra.kafka.streaming

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
import org.apache.kafka.streams.state.{ KeyValueBytesStoreSupplier, Stores }
import org.apache.kafka.streams.{ KafkaStreams, Topology }

object CapitalizeAggregateToStateStore extends App with TopologyDefinition {

  lazy val appId          = "streams-capitalize-aggregate"
  lazy val inputTopic     = "streams-plaintext-input"
  lazy val stateStoreName = "capital-text-store"

  // Create topology
  val topology: Topology = createTopology()

  // Examine the topology created above
  println(s"Topology is: ${topology.describe()}")

  // Init the stream with topology
  val streams = new KafkaStreams(topology, Util.kafkaStreamsProps(appId))

  // The program will run until it is aborted.
  // Execute a shutdown hook to close the stream before shutting down the app.
  sys.addShutdownHook(() => Util.closeStream(streams))

  println("Starting kafka stream ...")
  streams.start()

  override def createTopology() = {
    // 1. Create an in-memory state-store
    val capitalTextStoreSupplier: KeyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore(stateStoreName)

    // 2. Create a streams builder
    val builder = new StreamsBuilder()

    // 3. Create a source stream from the input topic
    //    The source stream is a `KStream` that is generating records from its source kafka topic (inputTopic)
    //    The records are organized as (String, String) key-value pairs
    val source = builder.stream[String, String](inputTopic)

    // 4. Simply pipe the contents from input topic, transform all text to UPPERCASE, and aggregate to state-store
    source
      .mapValues(value => value.toUpperCase)
      .groupByKey
      .aggregate("")((_, value, _) => value)(
        Materialized.as(capitalTextStoreSupplier).withKeySerde(Serdes.stringSerde).withValueSerde(Serdes.stringSerde)
      )
      .toStream
      .foreach((key, value) => println(s"$key: $value"))

    // 4. Build the topology
    builder.build()
  }
}
