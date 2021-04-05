package com.github.fescalhao.spark.example2

import com.github.fescalhao.SparkUtils._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

object FileStreamDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating Spark Session")
    val spark = SparkSession.builder()
      .config(getSparkConf("File Streaming Demo"))
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    val rawDF = spark.readStream
      .format("json")
      .option("path", "data/")
      .option("maxFilesPerTrigger", 1)
      .load()

    val explodedDF = rawDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID", "CustomerType", "PaymentMethod",
    "DeliveryType", "DeliveryAddress.City", "DeliveryAddress.State", "DeliveryAddress.PinCode",
      "explode(InvoiceLineItems) as LineItem")

    val flattenedDF = explodedDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    val invoiceWriteQuery = flattenedDF.writeStream
      .format("json")
      .option("path", "output/example2")
      .option("checkpointLocation", "chk-point-dir/example2")
      .outputMode(OutputMode.Append())
      .queryName("Flattened Invoice Write")
      .trigger(Trigger.ProcessingTime(Duration(1, TimeUnit.MINUTES)))
      .start()

    logger.info("Flattened Invoice Writer started...")
    invoiceWriteQuery.awaitTermination()
  }
}
