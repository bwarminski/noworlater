package org.brett.noworlater

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.typesafe.scalalogging.StrictLogging
import org.brett.noworlater.Messages.Remove

/**
  * Created by bwarminski on 10/26/17.
  */
object Consumer extends App with StrictLogging {
  logger.info("Starting")
  val kinesisClient = AmazonKinesisClientBuilder.standard().withEndpointConfiguration(new EndpointConfiguration("http://localhost:4567", "us-east-1")).build()
  val shard = "shardId-000000000000"
  val config = KinesisStreamConfig("test", shard, 16, 300, "1")
  val kinesis = new KinesisStream(config, kinesisClient)

  var iterator = Option(kinesis.getIterator(None))
  while (iterator.isDefined) {
    val records = kinesis.nextRecords(iterator.get)
    iterator = records.nextIterator
    for (event <- records.events) {
      event match {
        case r: Remove  => {
          for (id <- r.ids) logger.info(s"${id}")
        }
        case _ => ;
      }
    }
  }

}
