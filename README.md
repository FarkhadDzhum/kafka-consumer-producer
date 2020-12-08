
# JSON конфигурация:
**Заполняет <Разработчик>**

| Поле | Описание  |
| :---:   | :-: |
| Хэшированная JSON конфигурация | {"spark.executor.cores":1, <br/>"spark.executor.memory":"1g", <br/>"spark.cores.max":1 |
| Входные параметры | {"kafka.server":"kz-dmpkafka04:9092,kz-dmpkafka05:9092,kz-dmpkafka06:9092",<br/>"kafka.in.topic":"uss_kzmain_bssdbo_request",<br/>"kafka.in.username":"rsagadiyev",<br/>"kafka.in.password":"GE4su#F|",<br/>"kafka.in.groupid":"rsagadiyev",<br/>"kafka.out.topic":"rsagadiyev",<br/>"kafka.out.username":"rsagadiyev",<br/>"kafka.out.password":"GE4su#F|"} |


# Описание проекта:
Проект представляет собой пример элементарного триггера, который читает данные из одного Kafka-топика и передает их в другой.
<br/>В качестве примера был взят триггер error_tp_trigger.
<br/>Также в данном проекте расположены примеры элементарных Kafka Producer и Kafka Consumer.


# Прочее:
Данные по Kafka передаются в виде параметров.
<br/>Kafka Producer и Kafka Consumer запускаются как standalone-приложения.


# Среда запуска:
kz-dmpignt33 - 172.28.59.37
<br/>/mnt/nfs-spark-application/
<br/>java -jar dmc-hash-generator.jar 100142