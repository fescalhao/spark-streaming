package com.github.fescalhao.spark.schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object UserSchema {
  def getUserLoginJsonSchema: StructType = {
    StructType(List(
      StructField("created_time", StringType),
      StructField("login_id", StringType)
    ))
  }
}
