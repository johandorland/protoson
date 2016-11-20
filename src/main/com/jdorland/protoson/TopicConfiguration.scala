package com.jdorland.protoson

import com.google.protobuf.Descriptors.Descriptor

/**
  * Created by johan on 11/18/16.
  */
case class TopicConfiguration(sourceTopic: String, destinationTopic: String, descriptor: Descriptor) {

}
