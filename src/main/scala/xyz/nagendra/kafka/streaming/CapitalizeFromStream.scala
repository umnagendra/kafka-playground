package xyz.nagendra.kafka.streaming

import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
import org.apache.kafka.streams.state.{ KeyValueStore, StoreBuilder, Stores }
import org.apache.kafka.streams.{ KafkaStreams, Topology }

import scala.util.Try

object CapitalizeFromStream extends App with TopologyDefinition {

  lazy val appId          = "streams-capitalize-process"
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
    val capitalTextStoreBuilder: StoreBuilder[KeyValueStore[String, String]] =
      Stores.keyValueStoreBuilder[String, String](
        Stores.inMemoryKeyValueStore(stateStoreName),
        Serdes.stringSerde,
        Serdes.stringSerde
      )

    // 2. Create a streams builder
    val builder = new StreamsBuilder()

    // 3. Attach the state-store to the topology
    builder.addStateStore(capitalTextStoreBuilder)

    // 4. Create a source stream from the input topic
    //    The source stream is a `KStream` that is generating records from its source kafka topic (inputTopic)
    //    The records are organized as (String, String) key-value pairs
    val source = builder.stream[String, String](inputTopic)

    // 5. Simply pipe the contents from input topic, transform all text to UPPERCASE, and then process it
    source.mapValues(value => value.toUpperCase).process(() => new MyProcessor, stateStoreName)

    // 4. Build the topology
    builder.build()
  }
}

class MyProcessor extends AbstractProcessor[String, String] {

  override def process(key: String, value: String): Unit = {
    println(s"Adding to state-store: $value")

    // get store reference
    val store = this
      .context()
      .getStateStore(CapitalizeFromStream.stateStoreName)
      .asInstanceOf[KeyValueStore[String, String]]

    // add value to store
    store.put(key, value)

    // dump contents of store
    dumpStateStoreContents(store)
  }

  private def dumpStateStoreContents(store: KeyValueStore[String, String]): Unit = {
    println("State Store Contents:\n----------------")
    val range = store.all()
    try while (range.hasNext)
      Try(range.next()).foreach(kv => println(s"KEY: ${kv.key}\t\tVALUE: ${kv.value}"))
    finally range.close()
    println("\n")
  }
}
