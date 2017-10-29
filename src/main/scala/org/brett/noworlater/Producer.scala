package org.brett.noworlater

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.typesafe.scalalogging.StrictLogging
import org.brett.noworlater.Messages.DelayedMessage
import org.joda.time.DateTime

import scala.util.Try

/**
  * Created by bwarminski on 10/26/17.
  */
object Producer extends App with StrictLogging {
  logger.info("Starting")
  val kinesisClient = AmazonKinesisClientBuilder.standard().withEndpointConfiguration(new EndpointConfiguration("http://localhost:4567", "us-east-1")).build()
  val shard = "shardId-000000000000"
  val config = KinesisStreamConfig("test", shard, 300, "1")
  val kinesis = new KinesisStream(config, kinesisClient)
  var input = Option(scala.io.StdIn.readLine())
  while (input.isDefined) {
    val split = input.get.trim.split(' ')
    if (split.length != 2) {
      logger.error("Expected two entries")
    } else {
      val (id, offset) = (split(0), split(1))
      for (longOffset <- Try {offset.toLong}) {
        val deliverAt = DateTime.now().plus(longOffset)
        kinesis.add(DelayedMessage(id, deliverAt.getMillis, deliverAt.toString()))
      }
    }
    input = Option(scala.io.StdIn.readLine())
  }

}
