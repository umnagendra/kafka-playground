package xyz.nagendra.kafka.streaming

import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes.longSerde
import org.apache.kafka.streams.{TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties

class WordCountTest extends AnyFunSuite with BeforeAndAfterAll {

  test("Should print counts of each distinct word in incoming event payload text") {
    // arrange
    val topology: Topology                 = WordCount.createTopology()
    val props: Properties                  = Util.kafkaStreamsProps(WordCount.appId)
    val testDriver: TopologyTestDriver     = new TopologyTestDriver(topology, props)
    val stringSerde: StringSerde           = new StringSerde()
    val serializer: Serializer[String]     = stringSerde.serializer()
    val deserializer: Deserializer[String] = stringSerde.deserializer()

    val testInputTopic: TestInputTopic[String, String] =
      testDriver.createInputTopic(WordCount.inputTopic, serializer, serializer)
    val testOutputTopic: TestOutputTopic[String, Long] =
      testDriver.createOutputTopic(WordCount.outputTopic, deserializer, longSerde.deserializer())

    // act
    for (i <- 1 to 100) testInputTopic.pipeInput("the quick brown fox jumps over the lazy dog")

    // assert
    for (i <- 1 to 100) {
      val record = testOutputTopic.readKeyValue()
      println(s"record = ${record.key}:${record.value}")
    }

    testDriver.close()
  }
}
