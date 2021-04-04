package com.github.fescalhao.spark.example1

import com.github.fescalhao.SparkUtils._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingWC extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating Spark Session")
    val spark = SparkSession.builder()
      .config(getSparkConf("Streaming"))
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    val wordsDF = linesDF.select(expr("explode(split(value, ' ')) as word"))
    val countDF = wordsDF.groupBy("word").count()

    val wordCountQuery = countDF.writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir")
      .outputMode("complete")
      .start()

    logger.info("Listening to localhost:9999")
    wordCountQuery.awaitTermination()

  }
}
