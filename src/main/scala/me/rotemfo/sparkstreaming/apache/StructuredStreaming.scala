package me.rotemfo.sparkstreaming.apache

import java.sql.Timestamp
import java.util.regex.Matcher

import me.rotemfo.sparkstreaming.Utilities
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming.structured
 * file:    StructuredStreaming
 * created: 2019-11-08
 * author:  Rotem
 */
object StructuredStreaming {
  private final val logger: Logger = LoggerFactory.getLogger(getClass)

  case class LogEntry(ip: String,
                      client: String,
                      user: String,
                      dateTime: Timestamp,
                      request: String,
                      status: Int,
                      bytes: String,
                      referrer: String,
                      agent: String)

  private def parseLog(x: Row): Option[LogEntry] = {
    val matcher: Matcher = logPattern.matcher(x.getString(0))
    if (matcher.matches)
      Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        Utilities.parseDateField(matcher.group(4)).getOrElse(Utilities.now),
        matcher.group(5),
        matcher.group(6).toInt,
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    else None
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StructuredStreaming")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint/StructuredStreaming")
      .getOrCreate()


    val df = spark.readStream.text("logs")

    import spark.implicits._
    val structured = df.flatMap(parseLog).select("status", "dateTime")
    // val window = Window.partitionBy("status").orderBy("dateTime")
    val windowed = structured.groupBy($"status", window($"dateTime", "1 hour")).count().orderBy("window")
    val query = windowed.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
    spark.close()
  }
}
