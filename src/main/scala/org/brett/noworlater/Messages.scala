package org.brett.noworlater

import java.io.{InputStream, OutputStream}

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging

/**
  * Created by bwarminski on 10/26/17.
  */
object Messages extends StrictLogging {
  sealed trait Message {
    def seq: String
    def partitionKey: String
  }

  case class Add(message: DelayedMessage, seq: String) extends Message {
    val partitionKey = message.id
  }
  case class Remove(message: DelayedMessage, seq: String) extends Message {
    val partitionKey = message.id
  }
  case class Sync(id:String, seq: String, partitionKey: String) extends Message

  case class DelayedMessage(id: String, deliverAt: Long, payload: String)

  val objectMapper = new ObjectMapper()
  def deserialize(in: InputStream, sequence: String, partitionKey: String ): Option[Message] = {
    import scala.collection.JavaConverters._
    val json = objectMapper.readTree(in)
    json.get("type").asText().toLowerCase match {
      case "add" => Some(Add(DelayedMessage(json.get("id").asText, json.get("at").asLong(), json.get("data").asText), sequence))
      case "remove" => Some(Remove(DelayedMessage(json.get("id").asText, json.get("at").asLong(), json.get("data").asText), sequence))
      case "sync" => Some(Sync(json.get("id").asText(), sequence, partitionKey))
      case t : String => {logger.warn(s"Unable to match given type ${t}"); None}
    }
  }

  def add(message: DelayedMessage, out: OutputStream): Unit = {
    objectMapper.writeValue(out,
      objectMapper.createObjectNode()
        .put("type", "add")
        .put("id", message.id)
        .put("at", message.deliverAt)
        .put("data", message.payload)
    )
  }

  def remove(message: DelayedMessage, out: OutputStream): Unit = {
    objectMapper.writeValue(out,
      objectMapper.createObjectNode()
        .put("type", "remove")
        .put("id", message.id)
        .put("at", message.deliverAt)
        .put("data", message.payload)
    )
  }

  def sync(id: String, out: OutputStream): Unit = {
    objectMapper.writeValue(out, objectMapper.createObjectNode().put("type", "sync").put("id", id))
  }
}

