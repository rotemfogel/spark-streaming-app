package me.rotemfo.kafka

import java.util.concurrent.atomic.AtomicLong

import kafka.zk.{AdminZkClient, KafkaZkClient}
import me.rotemfo.common.Logging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Try

/**
 * project: spark-streaming-app
 * package: me.rotemfo.kafka
 * file:    KafkaPush
 * created: 2019-11-20
 * author:  Rotem
 */
object KafkaPush extends Logging {
  private final val topic: String = "logs"

  def main(args: Array[String]): Unit = {
    val source = Source.fromInputStream(getClass.getResourceAsStream("/access_log.txt"))
    val list = ArrayBuffer[String]()
    source.getLines().foreach(list +=)
    source.close()

    createTopic(topic)

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

    val succeeded: AtomicLong = new AtomicLong(0)
    val failed: AtomicLong = new AtomicLong(0)

    val callback = new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) failed.incrementAndGet()
        else succeeded.incrementAndGet()
      }
    }
    while (true) {
      list.foreach(l => producer.send(new ProducerRecord[String, String](topic, l.hashCode.toString, l), callback))
      logger.info(s"succeeded: $succeeded, failed: $failed")
    }
  }

  private def createTopic(topic: String, partitions: Int = 1, replicationFactor: Int = 1): Unit = {
    val zk = KafkaZkClient("127.0.0.1:2181", JaasUtils.isZkSecurityEnabled, 30, 30, Int.MaxValue, Time.SYSTEM)
    val admin = new AdminZkClient(zk)
    Try(admin.createTopic(topic, partitions, replicationFactor))
  }
}
