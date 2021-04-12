package com.github.fescalhao.spark.example10

import com.github.fescalhao.Utils.getSparkConf
import com.github.fescalhao.spark.schema.WebEventSchema._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json, to_timestamp}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object StreamToStreamJoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark = SparkSession
      .builder()
      .config(getSparkConf("Stream to Stream Join Demo"))
      .getOrCreate()

    logger.info("Defining impression json schema")
    val impressionSchema = getImpressionJsonSchema

    logger.info("Defining click json schema")
    val clickSchema = getClickJsonSchema

    logger.info("Defining impression kafka source")
    val impressionKafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "impressions")
      .option("startingOffsets", "earliest")
      .load()

    logger.info("Applying transformations to impression dataframe")
    val impressionDF = impressionKafkaSourceDF
      .select(from_json(col("value").cast("string"), impressionSchema).alias("value"))
      .select(
        col("value.ImpressionID"),
        col("value.CreatedTime"),
        col("value.Campaigner")
      )
      .withColumn("ImpressionTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
      .drop("CreatedTime")
      .withWatermark("ImpressionTime", "30 minute")

    logger.info("Defining click kafka source")
    val clickKafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "clicks")
      .option("startingOffsets", "earliest")
      .load()

    val clickDF = clickKafkaSourceDF
      .select(from_json(col("value").cast("string"), clickSchema).alias("value"))
      .select(
        col("value.ImpressionID"),
        col("value.CreatedTime")
      )
      .withColumn("ClickTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
      .drop("CreatedTime")
      .withWatermark("ClickTime", "30 minute")

    val joinExpr = (impressionDF.col("ImpressionID") === clickDF.col("ImpressionID")) &&
      (clickDF.col("ClickTime") between(impressionDF.col("ImpressionTime"), impressionDF.col("ImpressionTime") + expr("interval 15 minute")))

    val joinedDF = impressionDF
      .join(clickDF, joinExpr, "leftOuter")
      .drop(clickDF.col("ImpressionID"))

    val outputQuery = joinedDF
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "chk-point-dir/example10")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    outputQuery.awaitTermination()
  }
}
