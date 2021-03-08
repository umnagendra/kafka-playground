package xyz.nagendra.kafka.streaming

import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import org.apache.kafka.streams.{ TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver }
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties
import scala.collection.JavaConverters._

class LineSplitTest extends AnyFunSuite {

  test("Should split and emit each word from the incoming event text") {
    // arrange
    val topology: Topology                 = LineSplit.createTopology()
    val props: Properties                  = Util.kafkaStreamsProps(LineSplit.appId)
    val testDriver: TopologyTestDriver     = new TopologyTestDriver(topology, props)
    val stringSerde: StringSerde           = new StringSerde()
    val serializer: Serializer[String]     = stringSerde.serializer()
    val deserializer: Deserializer[String] = stringSerde.deserializer()

    val testInputTopic: TestInputTopic[String, String] =
      testDriver.createInputTopic(LineSplit.inputTopic, serializer, serializer)
    val testOutputTopic: TestOutputTopic[String, String] =
      testDriver.createOutputTopic(LineSplit.outputTopic, deserializer, deserializer)

    // act
    val sentence = "Hey, this is a sentence - it contains words."
    testInputTopic.pipeInput("TEST_KEY", sentence)

    // assert
    val records         = testOutputTopic.readValuesToList()
    val expectedRecords = sentence.split("\\W+").toList
    assert(records.size() == expectedRecords.size)
    assert(expectedRecords == records.asScala)
  }
}
