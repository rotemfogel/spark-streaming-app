// Kinesis setup steps (and more) at http://spark.apache.org/docs/latest/streaming-kinesis-integration.html

package me.rotemfo.sparkstreaming.kinesis

import java.util.Date

import com.amazonaws.services.kinesis.model.Record
import me.rotemfo.common.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.TrimHorizon
import org.apache.spark.streaming.kinesis._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object KinesisExample extends Logging {

  case class MyData(partitionKey: String, seqNumber: String, timeOfArrival: Date, data: Array[Byte])

  private final val regionName: String = "us-west-2"
  private final val endpointURL: String = s"kinesis.$regionName.amazonaws.com"
  private final val appName: String = "KinesisExample"
  private final val streamName: String = "production-events"

  private def millis(seconds: Int): Long = seconds * DateTimeUtils.MILLIS_PER_SECOND

  private def messageHandler(r: Record): MyData = {
    MyData(r.getPartitionKey,
      r.getSequenceNumber,
      r.getApproximateArrivalTimestamp,
      r.getData.array()
    )
  }

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val streamingContext = new StreamingContext("local[*]", appName, Seconds(5))

    val kinesisStream = KinesisInputDStream
      .builder
      .streamingContext(streamingContext)
      .endpointUrl(endpointURL)
      .regionName(regionName)
      .streamName(streamName)
      .initialPosition(new TrimHorizon)
      .checkpointAppName(appName)
      .checkpointInterval(Duration(millis(60)))
      .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
      .buildWithMessageHandler(messageHandler)

    kinesisStream.map(m => logger.info(s"$m"))
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

