package com.jdorland.protoson

import java.io.FileInputStream
import java.text.ParseException
import java.util
import java.util.{Collections, List, Properties}

import com.google.protobuf.DescriptorProtos.{FileDescriptorProto, FileDescriptorSet}
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}
import com.google.protobuf.DynamicMessage.Builder
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.{Descriptors, DynamicMessage}
import kafka.consumer.BaseConsumerRecord
import kafka.tools.MirrorMaker.MirrorMakerMessageHandler
import kafka.utils.Logging
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.Record

import scala.collection.JavaConverters

class ProtosonMessageHandler(propertiesFile: String) extends MirrorMakerMessageHandler with Logging {

  val topicMap: Map[String, TopicConfiguration] = parseConfiguration(propertiesFile)
  val emptyRecord = new util.ArrayList[ProducerRecord[Array[Byte], Array[Byte]]]()

  def parseConfiguration(propertiesFile: String): Map[String, TopicConfiguration] = {
    val propertiesMap = JavaConverters.propertiesAsScalaMapConverter {
      val properties: Properties = new Properties()
      properties.load(new FileInputStream(propertiesFile))
      properties
    }.asScala

    val configuration = propertiesMap.get("protoson.topics") match {
      case Some(topics) => topics.split(",").map(topic => {
        val descFile = propertiesMap.getOrElse("protoson." + topic + ".descriptor", topic + ".desc")
        val messageName = propertiesMap.getOrElse("protoson." + topic + ".message", topic)
        val destinationTopic = propertiesMap.getOrElse("protoson." + topic + ".destination", topic)

        (topic, TopicConfiguration(topic, destinationTopic, getDescriptor(descFile, messageName)))
      })

      case None => throw new ParseException("Expected property \"protoson.topics\" to be present", 0)
    }

    this.info("Started with the following configuration:" + configuration.map(topic => "\n" + topic._1 + " : " + topic._2))
    Map() ++ configuration
  }

  def getDescriptor(descFile: String, messageName: String): Descriptor = {
    val descriptorProto: FileDescriptorProto = FileDescriptorSet.parseFrom(new FileInputStream(descFile)).getFile(0)
    Descriptors.FileDescriptor.buildFrom(descriptorProto, Array.empty[FileDescriptor]).findMessageTypeByName(messageName)
  }

  override def handle(record: BaseConsumerRecord): List[ProducerRecord[Array[Byte], Array[Byte]]] = {
    this.trace("Received an event on topic: " + record.topic)
    this.trace("Payload: " + record.value)

    try {
      topicMap.get(record.topic) match {
        case Some(topicConfiguration) =>
          val builder: Builder = DynamicMessage.newBuilder(topicConfiguration.descriptor)
          JsonFormat.parser().merge(new String(record.value), builder)

          val dynamicMessage: DynamicMessage = builder.build()

          val timestamp: java.lang.Long = if (record.timestamp == Record.NO_TIMESTAMP) null else record.timestamp
          Collections.singletonList(new ProducerRecord[Array[Byte], Array[Byte]](topicConfiguration.destinationTopic, null, timestamp, record.key, dynamicMessage.toByteArray))

        case None => emptyRecord
      }
    } catch {
      case e: Exception =>
        this.info("An error occured when processing a request: " + e)
        emptyRecord
    }
  }
}