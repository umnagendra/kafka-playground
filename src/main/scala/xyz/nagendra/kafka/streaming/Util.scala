package xyz.nagendra.kafka.streaming

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.util.Properties

object Util {
  def kafkaStreamsProps(appId: String): Properties = {
    val props: Properties = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props
  }

  def closeStream(streams: KafkaStreams): Unit = {
    println(s"Executing shutdown hook... closing kafka stream")
    streams.close()
  }
}
