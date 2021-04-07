package com.github.fescalhao.spark.example6

import com.github.fescalhao.Utils.getSparkConf
import com.github.fescalhao.spark.schema.InvoiceSchema.{getFlattenedInvoiceAvroSchema, getInvoiceRewardsAvroSchema}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.functions.{col, struct, sum}
import org.apache.spark.sql.streaming.OutputMode

object KafkaAvroSourceDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark = SparkSession
      .builder()
      .config(getSparkConf("Kafka Avro Source Demo"))
      .getOrCreate()

    logger.info("Defining kafka source")
    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "flattened-invoices")
      .option("startingOffsets", "earliest")
      .load()

    logger.info("Getting invoice avro schema")
    val avroInvoiceSchema = getFlattenedInvoiceAvroSchema

    logger.info("Applying transformations")
    val valueDF = kafkaSourceDF.select(
      from_avro(col("value"), avroInvoiceSchema).alias("value")
    )

//    valueDF.show(false)

    val rewardsDF = valueDF
      .filter(col("value.CustomerType") === "PRIME")
      .groupBy(col("value.CustomerCardNo").alias("CustomerCardNo"))
      .agg(
        sum(col("value.TotalValue")).alias("TotalPurchase"),
        sum(col("value.TotalValue") * 0.2).cast("integer").alias("AggregatedRewards")
      )

//    rewardsDF.show(false)

    val kafkaTargetDF = rewardsDF.
      select(
        col("CustomerCardNo").alias("key"),
        to_avro(struct("CustomerCardNo", "TotalPurchase", "AggregatedRewards"), getInvoiceRewardsAvroSchema).alias("value")
      )

//    kafkaTargetDF.show(false)

    logger.info("Defining kafka target")
    val rewardsWriterQuery = kafkaTargetDF
      .writeStream
      .format("kafka")
      .queryName("Rewards Writer")
      .outputMode(OutputMode.Update())
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "customer-rewards")
      .option("checkpointLocation", "chk-point-dir/example6")
      .start()

    logger.info("Listening and writing on kafka")
    rewardsWriterQuery.awaitTermination()
  }
}
