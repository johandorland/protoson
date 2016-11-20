package com.jdorland.protoson

import kafka.consumer.BaseConsumerRecord
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ProtosonMessageHandlerTest extends FunSuite with Matchers {

  test("Initializing ProtosonMessageHandler with examples should work") {
    val handler = new ProtosonMessageHandler("examples/protoson.properties")
    handler should not be null
    handler.emptyRecord should have length 0
    handler.topicMap should have size 2
    handler.topicMap("testtopic").destinationTopic shouldEqual "prototesttopic"
    handler.topicMap("testtopic").sourceTopic shouldEqual "testtopic"
    handler.topicMap("scene").destinationTopic shouldEqual "scene"
    handler.topicMap("scene").sourceTopic shouldEqual "scene"
  }

  test("Handling valid messages should work") {
    val handler = new ProtosonMessageHandler("examples/protoson.properties")
    val testtopicRecord = BaseConsumerRecord("testtopic", 0, 0, key = "key".getBytes, value = "{\"company\":\"Awesome INC\",\"persons\":[{\"firstName\":\"Ann\",\"lastName\":\"Dryer\",\"residence\":{\"street\":\"bowlingstreet\",\"number\":1,\"zip\":\"1234 AB\"}},{\"firstName\":\"Bobby\",\"lastName\":\"Bobson\",\"residence\":{\"street\":\"Drowning street\",\"number\":10,\"zip\":\"5678 DC\"}},{\"firstName\":\"Chuck\",\"lastName\":\"Norris\",\"residence\":{\"street\":\"home\",\"number\":42,\"zip\":\"8421 CN\"}}]}".getBytes)
    val sceneRecord = BaseConsumerRecord("scene", 0, 0, timestamp = 1, key = "key".getBytes, value = "{\"title\":\"testscene\",\"objects\":{\"point\":{\"x\":1.0,\"y\":2.0,\"dimensions\":\"TWO\"},\"fly\":{\"x\":1.0,\"y\":2.0,\"z\":3.0,\"dimensions\":\"THREE\"}}}".getBytes)

    handler.handle(testtopicRecord) should have length 1
    handler.handle(testtopicRecord).get(0).timestamp() shouldEqual null

    handler.handle(sceneRecord) should have length 1
    handler.handle(sceneRecord).get(0).timestamp() shouldEqual 1
  }

  test("Handling invalid messages should work") {
    val handler = new ProtosonMessageHandler("examples/protoson.properties")
    val invalidTopic = BaseConsumerRecord("not a parsed topic", 0, 0, key = "key".getBytes, value = "".getBytes)
    val invalidRecord = BaseConsumerRecord("testtopic", 0, 0, key = "key".getBytes, value = "not valid json".getBytes)

    handler.handle(invalidTopic) should have length 0
    handler.handle(invalidRecord) should have length 0
  }
}