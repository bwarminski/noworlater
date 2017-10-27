package org.brett.noworlater

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.{GZIPInputStream, GZIPOutputStream, InflaterInputStream}

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.{GetRecordsRequest, PutRecordRequest, PutRecordsRequest, PutRecordsRequestEntry}
import com.google.common.io.ByteSource
import com.google.common.util.concurrent.RateLimiter
import com.typesafe.scalalogging.StrictLogging
import org.brett.noworlater.Messages.DelayedMessage

/**
  * Created by bwarminski on 10/26/17.
  */
object KinesisStream {
  val targetBytes = 1000000
  val initialBatchSize = 3600
  val batchSize: AtomicInteger = new AtomicInteger(initialBatchSize)
  val delta = 10
  val readRateLimiter = RateLimiter.create(5.0) // Allow 5 req/sec - per kinesis limits
}

case class StreamRecords(behindLatest: Long, nextIterator: Option[String], events: Seq[Messages.Message])
case class KinesisStreamConfig(stream: String, shard: String, maxBuckets: BigInt, batchSize: Int, syncHashKey: String)

/**
  * Basic single shard Kinesis reader implementation that rate limits and uses additive-increase-multiplicative-decrease to
  * size the getRecords requests appropriately. (Copy-paste-modified from Lucy code)
  *
  */
class KinesisStream(val config: KinesisStreamConfig, kinesis: AmazonKinesis) extends StrictLogging {
  import KinesisStream._

  import scala.collection.JavaConverters._


  def getIterator(after: Option[String]): String = {
    after match {
      case Some(offset) => kinesis.getShardIterator(config.stream, config.shard, "AFTER_SEQUENCE_NUMBER", offset).getShardIterator
      case None => kinesis.getShardIterator(config.stream, config.shard, "TRIM_HORIZON").getShardIterator
    }
  }

  def nextRecords(iterator: String): StreamRecords = {
    readRateLimiter.acquire()
    logger.info("Requesting {} records from stream", KinesisStream.batchSize.get())
    val records = kinesis.getRecords(new GetRecordsRequest().withLimit(KinesisStream.batchSize.get()).withShardIterator(iterator))
    val bytes = records.getRecords.asScala.map((r) => r.getData.remaining().toLong).sum
    logger.info("Read {} bytes from stream. {} ms behind latest.", bytes.toString, records.getMillisBehindLatest.toString)
    if (bytes < KinesisStream.targetBytes) {
      if (records.getMillisBehindLatest > 1000) {
        KinesisStream.batchSize.getAndUpdate((operand: Int) => (operand + KinesisStream.delta).min(10000))
      }
    } else {
      KinesisStream.batchSize.getAndUpdate((operand: Int) => (operand / 1.5).toInt)
    }

    StreamRecords(
      behindLatest = records.getMillisBehindLatest,
      nextIterator = Option(records.getNextShardIterator),
      events = records.getRecords.asScala
        .flatMap((r) => {
          Messages.deserialize(new GZIPInputStream(ByteSource.wrap(r.getData.array()).openStream()), r.getSequenceNumber, r.getPartitionKey)
        })
    )
  }

  def gzip(fn: (OutputStream) => Any): ByteBuffer = {
    val buffer = ByteBuffer.allocate(1024 * 1024 - 100) // Max Kinesis size + padding for partition key
    val outputStream = new GZIPOutputStream(new ByteBufferOutputStream(buffer))
    fn(outputStream)
    outputStream.close()
    buffer.flip()
    buffer
  }

  def add(messages: Seq[DelayedMessage]) = {
    val records = messages.groupBy((m) => {
      BigInt(MessageDigest.getInstance("MD5").digest(m.id.getBytes(StandardCharsets.UTF_8))) % config.maxBuckets
    }).flatMap((t) => {
      val (partitionKey, ids) = t
      ids.grouped(config.batchSize).map((group) => {
        new PutRecordsRequestEntry()
          .withPartitionKey(partitionKey.toString())
          .withExplicitHashKey(partitionKey.toString())
          .withData(gzip((out) => Messages.add(group, out)))
      })
    }).asJavaCollection
    kinesis.putRecords(new PutRecordsRequest().withStreamName(config.stream).withRecords(records)) // This is a little dirty, no retry or anything
  }

  def remove(ids: Seq[String]) = {
    val records = ids.groupBy((id) => {
      BigInt(MessageDigest.getInstance("MD5").digest(id.getBytes(StandardCharsets.UTF_8))) % config.maxBuckets
    }).flatMap((t) => {
      val (partitionKey, ids) = t
      ids.grouped(config.batchSize).map((group) => {
        new PutRecordsRequestEntry()
          .withPartitionKey(partitionKey.toString())
          .withExplicitHashKey(partitionKey.toString())
          .withData(gzip((out) => Messages.remove(group, out)))
      })
    }).asJavaCollection
    kinesis.putRecords(new PutRecordsRequest().withStreamName(config.stream).withRecords(records))
  }

  def sync(id: String) = {
    kinesis.putRecord(new PutRecordRequest()
      .withStreamName(config.stream)
      .withPartitionKey(config.syncHashKey)
      .withExplicitHashKey(config.syncHashKey)
      .withData(gzip((out) => Messages.sync(id, out))))
  }

}

