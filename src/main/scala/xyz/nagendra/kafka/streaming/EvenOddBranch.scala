package xyz.nagendra.kafka.streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, Serdes, StringSerializer}
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.serialization.Serdes.{javaIntegerSerde, stringSerde}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

object EvenOddBranch extends App with TopologyDefinition {

  val appId           = "streams-even-odd-branch"
  val inputTopic      = "streams-random"
  val outputTopicEven = "streams-output-even"
  val outputTopicOdd  = "streams-output-odd"

  // Create and describe topology
  val topology: Topology = createTopology()

  // Override the default kafka stream props to set Integer Serde for keys
  val props = Util.kafkaStreamsProps(appId)

  // Init the streams client and start it
  val streams = new KafkaStreams(topology, props)
  println(s"Topology is: ${topology.describe()}")

  // Also, start a producer thread that will keep injecting events into the input queue
  val producer = RandomEventProducer.initKafkaProducer()
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass)

  override def createTopology() = {
    // 1. Get the streams builder
    val builder = new StreamsBuilder()

    // 2. Create a source stream from the input topic
    //    The source stream is a `KStreamS` that is generating records from its source kafka topic (inputTopic)
    //    The records are organized as (Integer, String) key-value pairs
    val source = builder.stream[Integer, String](inputTopic)

    // 3. Branch out to two different stream processing branches (even and odd) based on Integer key
    val branches = source.branch(
      (key, _) => key % 2 == 0,
      (key, _) => key % 2 == 1
    )

    // 4.1 Process the events in even branch (all values in UPPERCASE)
    processBranch(branches(0),
                  value => value.toUpperCase(),
                  (key, value) => println(s"EVEN BRANCH:\t${key} -> ${value}"),
                  outputTopicEven
    )

    // 4.2 Process the events in even branch (all values in lowercase)
    processBranch(branches(1),
                  value => value.toLowerCase(),
                  (key, value) => println(s"ODD BRANCH:\t${key} -> ${value}"),
                  outputTopicOdd
    )

    // 5. Build the topology
    builder.build()
  }

  private def processBranch(branchStream: KStream[Integer, String],
                            processorFn: String => String,
                            peekFn: (Integer, String) => Unit,
                            outputTopic: String
  ): Unit =
    branchStream.mapValues(processorFn).peek(peekFn).to(outputTopic)(Produced.`with`(Serdes.Integer(), Serdes.String()))
  Future {
    println(s"Publishing events into ${inputTopic} topic ...")
    RandomEventProducer.produceRandomEvents(inputTopic, producer, 1000L)
  }

  // The program will run until it is aborted.
  // Execute a shutdown hook to close the stream before shutting down the app.
  sys.addShutdownHook { () =>
    producer.close()
    Util.closeStream(streams)
  }

  println("Starting kafka stream ...")
  streams.start()
}

object RandomEventProducer {
  private val random = new Random()

  def initKafkaProducer() =
    new KafkaProducer[Integer, String](kafkaProducerProps())

  private def kafkaProducerProps(): Properties = {
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all")
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProps
  }

  def produceRandomEvents(topic: String, producer: KafkaProducer[Integer, String], intervalMs: Long): Unit =
    while (true) {
      Thread.sleep(intervalMs)
      producer.send(new ProducerRecord[Integer, String](topic, random.nextInt(), random.nextString(32)))
    }
}
