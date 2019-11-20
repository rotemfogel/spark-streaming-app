package me.rotemfo.sparkstreaming

import java.util.regex.Matcher

import kafka.zk.{AdminZkClient, KafkaZkClient}
import me.rotemfo.sparkstreaming.Utilities._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming
 * file:    KafkaExample
 * created: 2019-11-20
 * author:  Rotem
 */
/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object KafkaExample {

  private def createTopic(topic: String, partitions: Int = 1, replicationFactor: Int = 1): Unit = {
    val zk = KafkaZkClient("127.0.0.1:2181", JaasUtils.isZkSecurityEnabled, 30, 30, Int.MaxValue, Time.SYSTEM)
    val admin =  new AdminZkClient(zk)
    Try(admin.createTopic(topic, partitions, replicationFactor))
  }

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "Kafka-logs-example", Seconds(1), environment = Map("spark.driver.bindAddress" -> "127.0.0.1"))

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topics = Set("logs")

    createTopic("logs")
    // Create our Kafka stream, which will contain (topic,message) pairs
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    val lines: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferBrokers, consumerStrategy)

    // Extract the request field from each log line
    val requests: DStream[Option[String]] = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x.value())
      if (matcher.matches()) Some(matcher.group(5)) else None
    })

    // Extract the URL from the request
    val urls = requests.filter(_.isDefined).map(x => {
      val arr = x.get.split(" ")
      if (arr.size == 3) arr(1) else "[error]"
    })

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
    sortedResults.print()

    // Kick it off
    ssc.checkpoint("checkpoint/KafkaSample")
    ssc.start()
    ssc.awaitTermination()
  }
}

