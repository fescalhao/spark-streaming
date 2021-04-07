package com.github.fescalhao.spark.example3

import com.github.fescalhao.Utils.getSparkConf
import com.github.fescalhao.spark.schema.InvoiceSchema.getInvoiceJsonSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object KafkaStreamDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(getSparkConf("Kafka Stream Demo"))
      .getOrCreate()

    val invoiceSchema = getInvoiceJsonSchema

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoices")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaDF.select(from_json(col("value").cast("string"), invoiceSchema).alias("value"))

    val explodedDF = valueDF.selectExpr(
      "value.InvoiceNumber",
      "value.CreatedTime",
      "value.StoreID",
      "value.PosID",
      "value.CustomerType",
      "value.PaymentMethod",
      "value.DeliveryType",
      "value.DeliveryAddress.City",
      "value.DeliveryAddress.State",
      "value.DeliveryAddress.PinCode",
      "explode(value.InvoiceLineItems) as LineItem"
    )

    val flattenedDF = explodedDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .queryName("Flattened Invoice Writer")
      .outputMode(OutputMode.Append())
      .option("path", "output/example3")
      .option("checkpointLocation", "chk-point-dir/example3")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Listening to Kafka")
    invoiceWriterQuery.awaitTermination()
  }
}


