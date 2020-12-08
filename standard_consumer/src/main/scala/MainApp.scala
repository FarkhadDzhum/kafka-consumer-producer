import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConversions._
import java.util.Properties


object MainApp {
  def main(args: Array[String]): Unit = {
    val props = new Properties()

// В кавычки нужно подставить необходимое имя пользователя, его пароль и имя топика
    val outusername: String = "username"
    val outpassword: String = "password"
    val outtopicid: String = "topic_id"

// Настройка параметров
// bootstrap.servers - необходимо подставить соответствующие адреса серверов Kafka
// group.id - необходимо подставить соответствующую группу чтения
    props.put("bootstrap.servers", "kz-dmpkafka04:9092,kz-dmpkafka05:9092,kz-dmpkafka06:9092")
    props.put("group.id", "group_id")
    props.put("enable.auto.commit", "true")
    props.put("auto.offset.reset", "earliest")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

// Настройка параметров авторизации
    props.put("security.protocol","SASL_PLAINTEXT")
    props.put("sasl.mechanism","PLAIN")
    props.put("compression.type", "none")
    props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";".format(outusername, outpassword))

// Создание объекта consumer класса KafkaConsumer
    val consumer = new KafkaConsumer(props)

// Создание объекта topicPartition класса TopiCPartition и подписка на топик outtopicid
    val topicPartition = new TopicPartition(outtopicid, 0)
    val topics = util.Arrays.asList(topicPartition)
    consumer.assign(topics)
    consumer.seekToBeginning(topics)

// Чтение сообщение в цикле
    while (true) {
      val records = consumer.poll(100)
      for (record <- records.iterator()) {
        println(record.value())
      }
    }
  }
}
