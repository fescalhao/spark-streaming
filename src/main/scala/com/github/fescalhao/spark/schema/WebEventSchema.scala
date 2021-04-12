package com.github.fescalhao.spark.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object WebEventSchema {
  def getImpressionJsonSchema: StructType = {
    StructType(List(
      StructField("ImpressionID", StringType),
      StructField("CreatedTime", StringType),
      StructField("Campaigner", StringType)
    ))
  }

  def getClickJsonSchema: StructType = {
    StructType(List(
      StructField("ImpressionID", StringType),
      StructField("CreatedTime", StringType)
    ))
  }
}
