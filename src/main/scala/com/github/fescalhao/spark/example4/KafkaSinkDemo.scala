package com.github.fescalhao.spark.example4

import com.github.fescalhao.Utils.getSparkConf
import com.github.fescalhao.spark.schema.InvoiceSchema.getInvoiceJsonSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, expr, from_json, struct, to_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.SparkSession

object KafkaSinkDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating Spark Session")
    val spark = SparkSession.builder()
      .config(getSparkConf("Kafka Sink Demo"))
      .getOrCreate()

    val invoiceSchema = getInvoiceJsonSchema

    logger.info("Defining kafka source")
    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoices")
      .option("startingOffset", "earliest")
      .load()

    logger.info("Applying transformations")
    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), invoiceSchema).alias("value"))

    val notificationDF = valueDF.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount")
      .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

    val kafkaTargetDF = notificationDF.select(col("InvoiceNumber").alias("key"),
      to_json(struct("CustomerCardNo", "TotalAmount", "EarnedLoyaltyPoints")).alias("value")
    )

//    kafkaTargetDF.show(false)

    logger.info("Defining kafka target")
    val notificationWriterQuery = kafkaTargetDF
      .writeStream
      .queryName("Notifications Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "notifications")
      .option("checkpointLocation", "chk-point-dir/example4")
      .outputMode(OutputMode.Append())
      .start()

    logger.info("Listening and writing to kafka")
    notificationWriterQuery.awaitTermination()
  }
}
