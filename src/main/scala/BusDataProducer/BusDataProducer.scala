package BusDataProducer

import scala.io.Source
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object Main extends App {

  def createProps(host: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", host)
    props.put("client.id", "BusDataProducer")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }


  def writeToKafka(line: String, topic: String, producer: KafkaProducer[String, String]): Unit = {
    val key = line.split(","){0} + " " + line.split(","){1}
    val record = new ProducerRecord[String, String](topic, key, line)

    producer.send(record).get()
  }


  def processRecord(topic: String, producer: KafkaProducer[String, String])(lastTimestamp: String, record: String): String = {
    val currentTimestamp = record.split(","){0} + " " + record.split(","){1}

    println(lastTimestamp, record)

//    if (currentTimestamp != lastTimestamp) { Thread.sleep(1000) }
    writeToKafka(record, topic, producer)

    currentTimestamp
  }


  val filePath = "../../data/LimitGPSData.csv"

  val props = createProps("10.129.34.90:9090")
  val producer = new KafkaProducer[String, String](props)

  val sourceFile = Source.fromFile(filePath)
  val file = sourceFile.getLines().drop(1)

  val busDataProducer: (String, String) => String = processRecord("raw-bus-data-9", producer)
  file.foldLeft("")(busDataProducer)

  producer.close()
  producer.flush()
  sourceFile.close()

}
