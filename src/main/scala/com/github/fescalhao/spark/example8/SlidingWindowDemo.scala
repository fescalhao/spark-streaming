package com.github.fescalhao.spark.example8

import com.github.fescalhao.Utils.getSparkConf
import com.github.fescalhao.spark.schema.SensorSchema.getSensorJsonSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, max, to_timestamp, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object SlidingWindowDemo {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark = SparkSession
      .builder()
      .config(getSparkConf("Sliding Window Demo"))
      .getOrCreate()

    logger.info("Defining sensor json schema")
    val sensorSchema = getSensorJsonSchema

    logger.info("Defining kafka source")
    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sensor")
      .option("startingOffsets", "earliest")
      .load()

    logger.info("Applying transformations")
    val valueDF = kafkaSourceDF.select(
      col("key").cast("string").alias("SensorID"),
      from_json(col("value").cast("string"), sensorSchema).alias("value")
    )

    val sensorDF = valueDF.select("SensorID", "value.*")
      .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))

    val aggDF = sensorDF
      .withWatermark("CreatedTime", "30 minute")
      .groupBy(
        col("SensorID"),
        window(col("CreatedTime"), "15 minute", "5 minute")
      )
      .agg(
        max(col("Reading")).alias("MaxReading")
      )

    val outputDF = aggDF
      .select("SensorID", "window.start", "window.end", "MaxReading")

    logger.info("Defining output target")
    val windowQuery = outputDF
      .writeStream
      .queryName("Sensor Write Query")
      .outputMode(OutputMode.Update())
      .format("console")
      .option("checkpointLocation", "chk-point-dir/example8")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Listening to kafka source...")
    windowQuery.awaitTermination()
  }
}
