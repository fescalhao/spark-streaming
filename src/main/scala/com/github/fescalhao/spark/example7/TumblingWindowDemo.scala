package com.github.fescalhao.spark.example7

import com.github.fescalhao.Utils.getSparkConf
import com.github.fescalhao.spark.schema.StockSchema.getStockJsonSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json, sum, to_timestamp, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object TumblingWindowDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark = SparkSession
      .builder()
      .config(getSparkConf("Tumbling Window Demo"))
      .getOrCreate()

    logger.info("Defining stock json schema")
    val stockSchema = getStockJsonSchema

    logger.info("Defining the kafka source")
    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "trades")
      .option("startingOffsets", "earliest")
      .load()

    logger.info("Applying transformations")
    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), stockSchema).alias("value"))

    val tradeDF = valueDF
      .select("value.*")
      .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end"))
      .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))

    val windowAggDF = tradeDF
      .withWatermark("CreatedTime", "30 minute")
      .groupBy(window(col("CreatedTime"), "15 minute"))
      .agg(
        sum(col("Buy")).alias("TotalBuy"),
        sum(col("Sell")).alias("TotalSell")
      )

    val outputDF = windowAggDF.select("window.start", "window.end", "TotalBuy", "TotalSell")

    logger.info("Defining output")
    val windowQuery = outputDF
      .writeStream
      .format("console")
      .queryName("Stock Writer")
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", "chk-point-dir/example7")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Counting Invoices")
    windowQuery.awaitTermination()
  }
}
