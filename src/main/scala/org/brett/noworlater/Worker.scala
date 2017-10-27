package org.brett.noworlater

import java.util.UUID

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.redis.RedisClient
import com.typesafe.scalalogging.StrictLogging

/**
  * Created by bwarminski on 10/25/17.
  */
object Worker extends App with StrictLogging {
  logger.info("Starting")
  val kinesisClient = AmazonKinesisClientBuilder.standard().withEndpointConfiguration(new EndpointConfiguration("http://localhost:4567", "us-east-1")).build()
  val shard = "shardId-000000000000"
  val config = KinesisStreamConfig("test", shard, 128, 300, "1")
  val kinesis = new KinesisStream(config, kinesisClient)

  val redis = new RedisClient("localhost", 6379)
  val worker = new Worker(kinesis, redis, SystemClock)
  worker.run()
//  val kinesisStream = new KinesisStream()
//  val worker = new Worker()
}

trait Clock {
  def currentTimeMillis: Long
}

object SystemClock extends Clock {
  override def currentTimeMillis: Long = System.currentTimeMillis()
}

class Worker(val kinesis: KinesisStream, val redis: RedisClient, val clock: Clock) extends StrictLogging {

  def run(): Unit = {
    import Messages._
    var syncUUID = UUID.randomUUID().toString
    kinesis.sync(syncUUID)

    val startingSequence = redis.hget("lastSequence", kinesis.config.shard)
    var iterator = Option(kinesis.getIterator(startingSequence))

    while (iterator.isDefined) {
      val records = kinesis.nextRecords(iterator.get)
      iterator = records.nextIterator
      for (event <- records.events) {
        event match {
          case a: Add if a.messages.nonEmpty => {
            val head = a.messages.head
            val tail = a.messages.tail
            redis.pipeline((multi) => {
              multi.sadd("partitions", a.partitionKey)
              multi.zadd(s"m:${a.partitionKey}", head.deliverAt.toDouble, head.id, tail.map((m) => (m.deliverAt.toDouble, m.id)): _*)
              multi.hset("lastSequence", kinesis.config.shard, a.seq)
            })
          }
          case r: Remove if r.ids.nonEmpty => {
            val head = r.ids.head
            val tail = r.ids.tail
            redis.pipeline((multi) => {
              multi.sadd("partitions", r.partitionKey)
              multi.zrem(s"m:${r.partitionKey}", head, tail: _*)
              multi.hset("lastSequence", kinesis.config.shard, r.seq)
            })
          }
          case s: Sync if s.id == syncUUID => {
            logger.info("Reached sync, dumping records")
            for (
              partitions <- redis.smembers("partitions");
              partitionOpt <- partitions;
              partition <- partitionOpt;
              entries <- redis.zrangebyscore(s"m:${partition}", max = clock.currentTimeMillis, limit = None)

            ) kinesis.remove(entries)
            redis.hset("lastSequence", kinesis.config.shard, s.seq)
            logger.info("sync complete")
            syncUUID = UUID.randomUUID().toString
            kinesis.sync(syncUUID)
          }
          case c: Message => logger.warn(s"Skipping message ${c}")
        }
      }

    }
  }
}
