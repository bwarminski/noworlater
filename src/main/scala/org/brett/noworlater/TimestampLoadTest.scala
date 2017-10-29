package org.brett.noworlater

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.codahale.metrics.{ConsoleReporter, CsvReporter}
import com.google.common.util.concurrent.RateLimiter
import com.typesafe.scalalogging.StrictLogging
import nl.grons.metrics.scala.DefaultInstrumented
import org.brett.noworlater.Messages.DelayedMessage
import org.joda.time.DateTime

import scala.util.Try

/**
  * Created by bwarminski on 10/27/17.
  */
object TimestampLoadTest extends App with StrictLogging with DefaultInstrumented {
  val eventsOut = metrics.meter("events-out")
  val reporter = CsvReporter.forRegistry(metricRegistry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build(new File("/Users/bwarminski/noworlater/benchmarks"));
  reporter.start(15, TimeUnit.SECONDS);
  logger.info("Starting")
  val kinesisClient = AmazonKinesisClientBuilder.standard().withEndpointConfiguration(new EndpointConfiguration("http://localhost:4567", "us-east-1")).build()
  val shard = "shardId-000000000000"
  val config = KinesisStreamConfig("test", shard, 300, "1")
  val kinesis = new KinesisStream(config, kinesisClient)
  var input = Option(scala.io.StdIn.readLine())
  val limiter = RateLimiter.create(200)
  while (input.isDefined) {
    limiter.acquire()
    val offset = input.get.trim.toLong
    val when = DateTime.now().plus(offset)
    kinesis.add(DelayedMessage(UUID.randomUUID().toString(), when.getMillis, when.toString()))
    eventsOut.mark()
    input = Option(scala.io.StdIn.readLine())
  }
}
