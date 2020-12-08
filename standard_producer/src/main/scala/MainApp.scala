import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import java.util.Properties

object MainApp {
  def main(args: Array[String]): Unit = {
    val props = new Properties()

// В кавычки нужно подставить необходимое имя пользователя, его пароль и имя топика
    val inusername: String = "username"
    val inpassword: String = "password"
    val intopicid: String = "topic_id"

// Настройка параметров
// bootstrap.servers - необходимо подставить соответствующие адреса серверов Kafka
// group.id - необходимо подставить соответствующую группу записи
    props.put("bootstrap.servers", "kz-dmpkafka04:9092,kz-dmpkafka05:9092,kz-dmpkafka06:9092")
    props.put("group.id", "group_id")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

// Настройка параметров авторизации
    props.put("security.protocol","SASL_PLAINTEXT")
    props.put("sasl.mechanism","PLAIN")
    props.put("compression.type", "none")
    props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";".format(inusername, inpassword))

// Создаем объект producer класса KafkaProducer
    val producer = new KafkaProducer[String, String](props)

// Записываем тестовое сообщение в топик Kafka
    producer.send(new ProducerRecord[String, String](intopicid, "Test sentence!"))

// Тестовая отправка сообщений в цикле
    for (i <- 1 to 10) {
      producer.send(new ProducerRecord[String, String](intopicid, Integer.toString(i)))
      Thread.sleep(500)
    }
    
    producer.close()
  }
}
