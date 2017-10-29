package org.brett.noworlater

import java.io.File
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.codahale.metrics.{ConsoleReporter, CsvReporter}
import com.redis.RedisClient
import com.typesafe.scalalogging.StrictLogging
import nl.grons.metrics.scala.DefaultInstrumented

/**
  * Created by bwarminski on 10/25/17.
  */
object Worker extends App with StrictLogging {
  logger.info("Starting")
  val kinesisClient = AmazonKinesisClientBuilder.standard().withEndpointConfiguration(new EndpointConfiguration("http://localhost:4567", "us-east-1")).build()
  val shard = "shardId-000000000000"
  val config = KinesisStreamConfig("test", shard, 300, "1")
  val kinesis = new KinesisStream(config, kinesisClient)

  val redis = new RedisClient("localhost", 6379)
  val worker = new Worker(kinesis, redis, SystemClock, 16)
  val reporter = CsvReporter.forRegistry(worker.metricRegistry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build(new File("/Users/bwarminski/noworlater/benchmarks"))
  reporter.start(15, TimeUnit.SECONDS)
  worker.run()
}

trait Clock {
  def currentTimeMillis: Long
}

object SystemClock extends Clock {
  override def currentTimeMillis: Long = System.currentTimeMillis()
}

class Worker(val kinesis: KinesisStream, val redis: RedisClient, val clock: Clock, val maxBuckets: BigInt) extends StrictLogging with DefaultInstrumented {
  val bucketStep = BigInt("340282366920938463463374607431768211455") / maxBuckets

  val kinesisGetTimer = metrics.timer("kinesis-get")
  val addTimer = metrics.timer("add")
  val removeTimer = metrics.timer("remove")
  val checkpointTimer = metrics.timer("checkpoint")
  val eventsIn = metrics.meter("events-in")
  val eventsOut = metrics.meter("events-out")
  val millisBehind = metrics.histogram("millis-behind")
  val rangeLookupTimer = metrics.timer("checkpoint-zrange")
  val dataGetTimer = metrics.timer("checkpoint-get")
  val kinesisWriteTimer = metrics.timer("kinesis-put")

  def run(): Unit = {
    import Messages._
    var syncUUID = UUID.randomUUID().toString
    kinesis.sync(syncUUID)

    val startingSequence = redis.hget("lastSequence", kinesis.config.shard)
    var iterator = Option(kinesis.getIterator(startingSequence))

    while (iterator.isDefined) {
      val records = kinesisGetTimer.time { kinesis.nextRecords(iterator.get) }
      eventsIn.mark(records.events.size)
      millisBehind.+=(records.behindLatest)
      iterator = records.nextIterator
      for (event <- records.events) {
        event match {
          case a: Add  => {
            val b = bucket(a.message.id)
            addTimer.time { redis.pipeline((multi) => {
              multi.sadd("partitions", b)
              multi.zadd(s"m:${b}", a.message.deliverAt.toDouble, a.message.id)
              multi.set(s"d:${a.message.id}", a.message.payload)
              multi.hset("lastSequence", kinesis.config.shard, a.seq)
            })}
          }
          case r: Remove => {
            val b = bucket(r.message.id)
            removeTimer.time { redis.pipeline((multi) => {
              multi.sadd("partitions", b)
              multi.zrem(s"m:${b}", r.message.id)
              multi.del(s"d:${r.message.id}")
              multi.hset("lastSequence", kinesis.config.shard, r.seq)
            })}
          }
          case s: Sync if s.id == syncUUID => checkpointTimer.time{
//            redis.evalMultiBulk()
            for (
              partitions <- redis.smembers("partitions");
              partitionOpt <- partitions;
              partition <- partitionOpt;
              entries <- redis.zrangebyscoreWithScore(s"m:${partition}", max = clock.currentTimeMillis, limit = None);
              entry <- entries
            ) {
              val (id, deliverAt) = entry
              val payload = dataGetTimer.time { redis.get(s"d:$id").getOrElse("") }
              val message = DelayedMessage(id, deliverAt.toLong, payload)
              kinesisWriteTimer.time { kinesis.remove(message) }
              eventsOut.mark()
            }
            redis.hset("lastSequence", kinesis.config.shard, s.seq)

            syncUUID = UUID.randomUUID().toString
            kinesis.sync(syncUUID)
          }
          case c: Message => logger.warn(s"Skipping message ${c}")
        }
      }

    }
  }

  def bucket(key:String): String = {

    ((BigInt(1, MessageDigest.getInstance("MD5").digest(key.getBytes(StandardCharsets.UTF_8))) % maxBuckets) * bucketStep).toString
  }
}
