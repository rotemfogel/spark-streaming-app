package me.rotemfo.sparkstreaming.cassandra

import java.util.regex.Matcher

import me.rotemfo.common.Logging
import me.rotemfo.sparkstreaming.Utilities.apacheLogPattern
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Listens to Apache log data on port 9999 and saves URL, status, and user agent
 * by IP address in a Cassandra database.
 */
object CassandraExample extends Logging {

  def main(args: Array[String]) {

    // Set up the Cassandra host address
    val conf = new SparkConf()
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    conf.setMaster("local[*]")
    conf.setAppName("CassandraExample")

    // Create the context with a 10 second batch size
    val ssc = new StreamingContext(conf, Seconds(10))

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "CassandraExample"
    )
    // List of topics you want to listen for from Kafka
    val topics = Set("logs")

    // Create our Kafka stream, which will contain (topic,message) pairs
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    val lines: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferBrokers, consumerStrategy)

    // Extract the (IP, URL, status, useragent) tuples that match our schema in Cassandra
    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x.value())
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val request = matcher.group(5)
        val requestFields = request.toString.split(" ")
        val url = scala.util.Try(requestFields(1)) getOrElse "[error]"
        (ip, url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", "error", 0, "error")
      }
    })

    import com.datastax.spark.connector._

    // Now store it in Cassandra
    requests.foreachRDD((rdd, _) => {
      rdd.cache()
      logger.info("Writing " + rdd.count() + " rows to Cassandra")
      rdd.saveToCassandra("sparkstreaming", "access_log", SomeColumns("IP", "URL", "Status", "UserAgent"))
    })

    // Kick it off
    ssc.checkpoint("checkpoint/CassandraExample")
    ssc.start()
    ssc.awaitTermination()
  }
}
