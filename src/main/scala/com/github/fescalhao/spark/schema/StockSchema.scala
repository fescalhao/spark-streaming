package com.github.fescalhao.spark.schema

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StockSchema {
  def getStockJsonSchema: StructType = {
    StructType(List(
      StructField("CreatedTime", StringType),
      StructField("Type", StringType),
      StructField("Amount", IntegerType),
      StructField("BrokerCode", StringType)
    ))
  }
}
