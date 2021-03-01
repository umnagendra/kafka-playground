package xyz.nagendra.kafka.streaming

import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import org.apache.kafka.streams.{ TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver }
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties

class CapitalizedPipeTest extends AnyFunSuite {

  test("Should capitalize all incoming event payload text") {
    // arrange
    val topology: Topology                 = CapitalizedPipe.createTopology()
    val props: Properties                  = Util.kafkaStreamsProps(CapitalizedPipe.appId)
    val testDriver: TopologyTestDriver     = new TopologyTestDriver(topology, props)
    val stringSerde: StringSerde           = new StringSerde()
    val serializer: Serializer[String]     = stringSerde.serializer()
    val deserializer: Deserializer[String] = stringSerde.deserializer()

    val testInputTopic: TestInputTopic[String, String] =
      testDriver.createInputTopic(CapitalizedPipe.inputTopic, serializer, serializer)
    val testOutputTopic: TestOutputTopic[String, String] =
      testDriver.createOutputTopic(CapitalizedPipe.outputTopic, deserializer, deserializer)

    // act
    for (i <- 1 to 100) testInputTopic.pipeInput(s"KEY$i", s"value-random-$i")

    // assert
    for (i <- 1 to 100) {
      val record = testOutputTopic.readKeyValue()
      println(s"Validating output record: ${record.value}")
      assert(record.value.toUpperCase == record.value, "Expected ALL values in output stream to be capitalized")
    }
  }
}
