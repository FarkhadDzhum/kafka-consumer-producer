import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.types.{StringType, StructType}
import com.google.gson.Gson
import java.text.SimpleDateFormat
import kz.dmc.packages.enums.DMCStatuses
import kz.dmc.packages.spark.DMCSpark

object Standard_Trigger {
  var json = new Gson()
  var flag = 0
  val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
  val event_id_generator = new SimpleDateFormat("yyyyMMddHHmmssSSSSSS")

  //val batchDuration: Int = DMCSpark.get.getAppParam("batchDuration").getAsString.toInt

  val kafkaServer = DMCSpark.get.getAppParam("kafka.server").getAsString
  val inusername: String = DMCSpark.get.getAppParam("kafka.in.username").getAsString
  val inpassword: String = DMCSpark.get.getAppParam("kafka.in.password").getAsString
  val intopicid = Array(DMCSpark.get.getAppParam("kafka.in.topic").getAsString)
  val ingroupid: String = DMCSpark.get.getAppParam("kafka.in.groupid").getAsString

  val outusername: String = DMCSpark.get.getAppParam("kafka.out.username").getAsString
  val outpassword: String = DMCSpark.get.getAppParam("kafka.out.password").getAsString
  val outtopicid: String = DMCSpark.get.getAppParam("kafka.out.topic").getAsString


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> kafkaServer,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> ingroupid,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "security.protocol" -> "SASL_PLAINTEXT",
    "sasl.mechanism" -> "PLAIN",
    "sasl.jaas.config" -> "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";".format(inusername, inpassword)
  )


  val schema = new StructType()
    .add("REQUEST_TYPE", StringType, nullable = false)
    .add("SYS_CREATION_DATE", StringType, nullable = true)
    .add("OPERATOR_ID", StringType, nullable = true)
    .add("REQUEST_STATUS", StringType, nullable = true)
    .add("REQUEST_CONTENT", StringType, nullable = true)



  def runTask(ss: org.apache.spark.sql.SparkSession): Unit = {
    try {
      val sparkConf = ss.sparkContext
      val spark = ss
      val ssc = new StreamingContext(sparkConf, Seconds(20))

      val dstream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](intopicid, kafkaParams)
      )

      dstream.foreachRDD(rawRDD => {
        val offsetRanges = rawRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        if (!rawRDD.isEmpty) {
          val df = spark.read.schema(schema).json(rawRDD.map(_.value())).toDF
          val filtered_df = df.filter(df("REQUEST_TYPE")==="MPPC" && df("REQUEST_STATUS")==="R")
          filtered_df.createOrReplaceTempView("uss")

          spark.sql(
            """
            SELECT
            'standard_trigger_test' as TRANSACTION_NAME,
            (DATE_FORMAT(CAST(UNIX_TIMESTAMP(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS") AS TIMESTAMP), "yyyyMMddHHmmssSSSSSS") || t.OPERATOR_ID) as EVENT_ID,
            trim(nvl(t.OPERATOR_ID,'')) as MSISDN,
            DATE_FORMAT(CAST(UNIX_TIMESTAMP(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS") AS TIMESTAMP), "yyyy-MM-dd'T'HH:mm:ss") as EVENT_TIMESTAMP,
            DATE_FORMAT(CAST(UNIX_TIMESTAMP(trim(t.SYS_CREATION_DATE), "yyyy-MM-dd HH:mm:ss.S") AS TIMESTAMP), "yyyy-MM-dd'T'HH:mm:ss") AS SOURCE_EVENT_TIMESTAMP,
            'DMP' as SOURCE,
            nvl(SUBSTR(t.REQUEST_CONTENT, INSTR(t.REQUEST_CONTENT,'<ban>')+5, INSTR(SUBSTR(t.REQUEST_CONTENT,INSTR(t.REQUEST_CONTENT,'<ban>')+5),'</ban>')-1), '') as BAN,
            '' as VEON_ID,
            1 as P1,
            0 as P2,
            nvl(SUBSTR(t.REQUEST_CONTENT, INSTR(t.REQUEST_CONTENT,'<soc>')+5, INSTR(SUBSTR(t.REQUEST_CONTENT,INSTR(t.REQUEST_CONTENT,'<soc>')+5),'</soc>')-1), '') as P3,
            '' as P4,
            '' as P5,
            '' as P6,
            '' as P7,
            0.0 as P8,
            0.0 as P9,
            0.0 as MAIN_BALANCE,
            0.0 as B1,
            0.0 as B2,
            0.0 as B3,
            0.0 as B4
            FROM uss t
        """
          ).toJSON
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaServer)
            .option("topic", outtopicid)
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.security.protocol", "SASL_PLAINTEXT")
            .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";".format(outusername, outpassword))
            .save()

        }
        dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })

      println("start spark streaming")
      ssc.start()
      ssc.awaitTermination()
      println("end spark streaming")
    } catch {
      case e: Exception => DMCSpark.get.computingLog(DMCStatuses.COMPUTING_WARNING, e.getMessage)
    }
  }
}
