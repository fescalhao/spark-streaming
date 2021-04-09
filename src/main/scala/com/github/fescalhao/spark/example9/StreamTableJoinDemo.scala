package com.github.fescalhao.spark.example9

import com.datastax.spark.connector.CassandraSparkExtensions
import com.github.fescalhao.Utils.getSparkConf
import com.github.fescalhao.spark.schema.UserSchema.getUserLoginJsonSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, to_timestamp}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType

object StreamTableJoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark = SparkSession
      .builder()
      .config(getSparkConf("Stream Table Join Demo"))
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .withExtensions(new CassandraSparkExtensions)
      .getOrCreate()

    logger.info("Defining user login json schema")
    val loginSchema = getUserLoginJsonSchema

    logger.info("Defining kafka source")
    val kafkaSourceDF = getKafkaSource(spark, "logins")

    logger.info("Applying transformations")
    val loginDF = transformKafkaSourceData(kafkaSourceDF, loginSchema)

    logger.info("Defining cassandra source")
    val userDF = getCassandraSource(spark)

    logger.info("Performing join")
    val joinExpr = loginDF.col("login_id") === userDF.col("login_id")
    val joinType = "inner"

    val joinedDF = loginDF.join(userDF, joinExpr, joinType)
      .drop(loginDF.col("login_id"))

    val outputDF = joinedDF.select(
      col("login_id"),
      col("user_name"),
      col("created_time").alias("last_login")
    )

    logger.info("Defining the target to update cassandra records")
    val outputQuery =
//      loginDF
//      .writeStream
//      .format("console")
//      .outputMode(OutputMode.Append())
//      .option("checkpointLocation", "chk-point-dir/example9")
//      .start()
      getOutputQuery(outputDF)

    logger.info("Waiting for query")
    outputQuery.awaitTermination()
  }

  def getKafkaSource(spark: SparkSession, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
  }

  def getCassandraSource(spark: SparkSession): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "spark_db")
      .option("table", "users")
      .load()
  }

  def transformKafkaSourceData(kafkaSourceDF: DataFrame, loginSchema: StructType): DataFrame = {
    val valueDF = kafkaSourceDF
      .select(from_json(col("value").cast("string"), loginSchema).alias("value"))

    valueDF.select("value.*")
      .withColumn("created_time", to_timestamp(col("created_time"), "yyyy-MM-dd HH:mm:ss"))
  }

  def getOutputQuery(outputDF: DataFrame): StreamingQuery = {
    outputDF
      .writeStream
      .foreachBatch(writeToCassandra _)
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", "chk-point-dir/example9")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
  }

  def writeToCassandra(outputDF: DataFrame, batchID: Long): Unit = {
    outputDF
      .write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "spark_db")
      .option("table", "users")
      .mode(SaveMode.Append)
      .save()
  }
}
