package xyz.nagendra.kafka.streaming

import org.apache.kafka.streams.Topology

trait TopologyDefinition {
  def createTopology(): Topology
}
