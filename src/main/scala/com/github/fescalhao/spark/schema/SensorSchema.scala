package com.github.fescalhao.spark.schema

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SensorSchema {
  def getSensorJsonSchema: StructType = {
    StructType(List(
      StructField("CreatedTime", StringType),
      StructField("Reading", DoubleType)
    ))
  }
}
