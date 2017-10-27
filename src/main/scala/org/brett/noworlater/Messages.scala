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

  case class Add(messages:Seq[DelayedMessage], seq: String, partitionKey: String) extends Message
  case class Remove(ids:Seq[String], seq: String, partitionKey: String) extends Message
  case class Sync(id:String, seq: String, partitionKey: String) extends Message

  case class DelayedMessage(id: String, deliverAt: Long)

  val objectMapper = new ObjectMapper()
  def deserialize(in: InputStream, sequence: String, partitionKey: String ): Option[Message] = {
    import scala.collection.JavaConverters._
    val json = objectMapper.readTree(in)
    json.get("type").asText().toLowerCase match {
      case "add" => {
        val messages = json.get("messages").asScala.toSeq.map((m) => DelayedMessage(m.get("id").asText, m.get("at").asLong()))
        Some(Add(messages, sequence, partitionKey))
      }
      case "remove" => Some(Remove(json.get("ids").asScala.toSeq.map(_.asText()), sequence, partitionKey))
      case "sync" => Some(Sync(json.get("id").asText(), sequence, partitionKey))
      case t : String => {logger.warn(s"Unable to match given type ${t}"); None}
    }
  }

  def add(messages: Seq[DelayedMessage], out: OutputStream): Unit = {
    val messageArr = objectMapper.createArrayNode()
    for (message <- messages) {
      val json = objectMapper.createObjectNode().put("id", message.id).put("at", message.deliverAt)
      messageArr.add(json)
    }
    objectMapper.writeValue(out, objectMapper.createObjectNode().put("type", "add").set("messages", messageArr))
  }

  def remove(ids: Seq[String], out: OutputStream): Unit = {
    val jsonIds = objectMapper.createArrayNode()
    ids.foreach((id) => jsonIds.add(id))
    objectMapper.writeValue(out, objectMapper.createObjectNode().put("type", "remove").set("ids", jsonIds))
  }

  def sync(id: String, out: OutputStream): Unit = {
    objectMapper.writeValue(out, objectMapper.createObjectNode().put("type", "sync").put("id", id))
  }
}

