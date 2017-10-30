package org.brett.noworlater

import java.io.File
import java.lang.Double
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.codahale.metrics.{ConsoleReporter, CsvReporter}
import com.typesafe.scalalogging.StrictLogging
import nl.grons.metrics.scala.DefaultInstrumented
import redis.clients.jedis.{Jedis, PipelineBase}

/**
  * Created by bwarminski on 10/25/17.
  */
object Worker extends App with StrictLogging {
  logger.info("Starting")
  val kinesisClient = AmazonKinesisClientBuilder.standard().withEndpointConfiguration(new EndpointConfiguration("http://localhost:4567", "us-east-1")).build()
  val shard = "shardId-000000000000"
  val config = KinesisStreamConfig("test", shard, 300, "1")
  val kinesis = new KinesisStream(config, kinesisClient)

  val redis = new Jedis("localhost", 6379)
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

class Worker(val kinesis: KinesisStream, val redis: Jedis, val clock: Clock, val maxBuckets: BigInt) extends StrictLogging with DefaultInstrumented {
  import scala.collection.JavaConverters._

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
  val ingestTimer = metrics.timer("ingest")
  val syncTimer = metrics.timer("sync")

  def run(): Unit = {
    import Messages._
    var syncUUID = UUID.randomUUID().toString
    kinesis.sync(syncUUID)

    val startingSequence = Option(redis.hget("lastSequence", kinesis.config.shard))
    var kinesisIterator = Option(kinesis.getIterator(startingSequence))

    while (kinesisIterator.isDefined) {
      val records = kinesisGetTimer.time { kinesis.nextRecords(kinesisIterator.get) }
      eventsIn.mark(records.events.size)
      millisBehind.+=(records.behindLatest)
      kinesisIterator = records.nextIterator

      val eventsIterator = records.events.iterator
      def ingest: Option[Sync] = {
        var checkpoint: Option[Sync] = None
        var adds: Vector[Add] = Vector()
        var removes: Vector[Remove] = Vector()
        while (eventsIterator.hasNext && checkpoint.isEmpty) {
          eventsIterator.next() match {
            case a: Add => adds = adds :+ a
            case r: Remove => removes = removes :+ r
            case s: Sync if s.id == syncUUID => checkpoint = Some(s)
            case c: Message => logger.warn(s"Skipping message ${c}")
          }
        }

        if (adds.nonEmpty || removes.nonEmpty) ingestTimer.time {
          val bucketedAdds = adds.groupBy((a) => bucket(a.message.id))
          val bucketedRemoves = removes.groupBy((a) => bucket(a.message.id))
          val multi = redis.pipelined()
          multi.multi()
          JedisHelper.saddStringPipeline(multi, "partitions", (bucketedAdds.keySet ++ bucketedRemoves.keySet).toSeq: _*)
          if (bucketedAdds.nonEmpty) {
            for ((b, a) <- bucketedAdds) {
              val scores = a.foldLeft(Map[String, Double]())((map, add) => map + (add.message.id -> add.message.deliverAt.toDouble)).asJava
              val data = a.foldLeft(Map[String, String]())((map, add) => map + (add.message.id -> add.message.payload)).asJava
              multi.zadd(s"m:${b}", scores)
              multi.hmset(s"d:${b}", data)
            }
          }
          if (bucketedRemoves.nonEmpty) {
            for ((b, r) <- bucketedRemoves) {
              val ids = r.map(_.message.id)
              multi.zrem(s"m:${b}", ids: _*)
              multi.hdel(s"d:${b}", ids: _*)
            }
          }
          multi.exec()
          multi.sync()
        }
        checkpoint
      }
      var checkpoint: Option[Sync] = ingest

      syncTimer.time {
        for (
          s <- checkpoint;
          partition <- redis.smembers("partitions").asScala
        ) {
          val tuples = redis.zrangeByScoreWithScores(s"m:${partition}", Double.NEGATIVE_INFINITY, clock.currentTimeMillis).asScala.toSeq
          if (tuples.nonEmpty) {
            val data = redis.hmget(s"d:${partition}", tuples.map(_.getElement).toSeq: _*).asScala
            kinesis.removeAll(tuples.zip(data).map((z) => {
              val (tuple, data) = z
              DelayedMessage(tuple.getElement, tuple.getScore.toLong, data)
            }))
          }
        }
      }
      if (checkpoint.isDefined) {
        syncUUID = UUID.randomUUID().toString
        kinesis.sync(syncUUID)
      }

      ingest

      for (last <- records.events.lastOption) redis.hset("lastSequence", kinesis.config.shard, last.seq)

    }
  }

  def bucket(key:String): String = {

    ((BigInt(1, MessageDigest.getInstance("MD5").digest(key.getBytes(StandardCharsets.UTF_8))) % maxBuckets) * bucketStep).toString
  }
}
