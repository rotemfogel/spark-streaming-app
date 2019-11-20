package me.rotemfo.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * project: spark-streaming-app
 * package: me.rotemfo.kafka
 * file:    KafkaPush
 * created: 2019-11-20
 * author:  Rotem
 */
object KafkaPush {
  private final val logger: Logger = LoggerFactory.getLogger(getClass)
  private final val topic: String = "logs"

  def main(args: Array[String]): Unit = {
    val source = Source.fromInputStream(getClass.getResourceAsStream("/access_log.txt"))
    val list = ArrayBuffer[String]()
    source.getLines().foreach(list +=)
    source.close()

    val config = new java.util.HashMap[String, Object]() {
      {
        put("bootstrap.servers", "localhost:9092")
        put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        put("max.block.ms", "2000")
      }
    }
    val producer = new KafkaProducer[String, String](config)
    sys.addShutdownHook {
      import scala.collection.JavaConverters._
      logger.info(s"Close Kafka producer for NotificationPayload, config: ${config.asScala}")
      producer.close()
    }

    while (true) {
      list.foreach(l => producer.send(new ProducerRecord[String, String](topic, l.hashCode.toString, l)))
    }
  }
}
