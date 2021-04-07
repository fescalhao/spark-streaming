package com.github.fescalhao.spark.example5

import com.github.fescalhao.Utils.getSparkConf
import com.github.fescalhao.spark.schema.InvoiceSchema.{getFlattenedInvoiceAvroSchema, getInvoiceJsonSchema}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions.{col, expr, from_json, struct}
import org.apache.spark.sql.streaming.OutputMode

object KafkaAvroSinkDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating Spark Session")
    val spark = SparkSession
      .builder()
      .config(getSparkConf("Kafka Avro Sink Demo"))
      .getOrCreate()

    logger.info("Defining json invoice schema")
    val invoiceJsonSchema = getInvoiceJsonSchema

    logger.info("Defining kafka source")
    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoices")
      .option("startingOffsets", "earliest")
      .load()

    logger.info("Applying transformations")
    val valueDF = kafkaSourceDF
      .select(
        from_json(col("value").cast("string"), invoiceJsonSchema).alias("value")
      )

    val explodedDF = valueDF.selectExpr(
      "value.InvoiceNumber",
      "value.CreatedTime",
      "value.StoreID",
      "value.PosID",
      "value.CustomerType",
      "value.CustomerCardNo",
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
      .withColumn("ItemPrice", expr("LineItem.ItemPrice").cast("double"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.ItemCode").cast("double"))
      .drop("LineItem")

    val kafkaTargetDF = flattenedDF
      .select(
        col("InvoiceNumber").alias("key"),
        to_avro(struct("InvoiceNumber", "CreatedTime", "StoreID", "PosID", "CustomerType", "CustomerCardNo", "PaymentMethod",
          "DeliveryType", "City", "State", "PinCode", "ItemCode", "ItemDescription", "ItemPrice", "ItemQty", "TotalValue"), getFlattenedInvoiceAvroSchema).alias("value")
      )

    logger.info("Defining kafka target")
    val flattenedInvoiceWriterQuery = kafkaTargetDF
      .writeStream
      .format("kafka")
      .queryName("Flattened Invoice Writer")
      .outputMode(OutputMode.Append())
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "flattened-invoices")
      .option("checkpointLocation", "chk-point-dir/example5")
      .start()

    logger.info("Listening and writing to kafka")
    flattenedInvoiceWriterQuery.awaitTermination()
  }
}
