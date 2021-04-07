package com.github.fescalhao.spark.schema

import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object InvoiceSchema {
  def getInvoiceJsonSchema: StructType = {
    StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      ))))
    ))
  }

  def getFlattenedInvoiceAvroSchema: String = {
    """|{
      |  "type": "record",
      |  "name": "FlattenedInvoice",
      |  "namespace": "com.github.fescalhao",
      |  "fields": [
      |    {"name": "InvoiceNumber","type": ["string", "null"]},
      |    {"name": "CreatedTime","type": ["long", "null"]},
      |    {"name": "StoreID","type": ["string", "null"]},
      |    {"name": "PosID","type": ["string", "null"]},
      |    {"name": "CustomerType","type": ["string", "null"]},
      |    {"name": "CustomerCardNo","type": ["string", "null"]},
      |    {"name": "PaymentMethod","type": ["string", "null"]},
      |    {"name": "DeliveryType","type": ["string", "null"]},
      |    {"name": "City","type": ["string", "null"]},
      |    {"name": "State","type": ["string", "null"]},
      |    {"name": "PinCode","type": ["string", "null"]},
      |    {"name": "ItemCode","type": ["string", "null"]},
      |    {"name": "ItemDescription","type": ["string", "null"]},
      |    {"name": "ItemPrice","type": ["double", "null"]},
      |    {"name": "ItemQty","type": ["int", "null"]},
      |    {"name": "TotalValue","type": ["double", "null"]}
      |  ]
      |}""".stripMargin
  }

  def getInvoiceRewardsAvroSchema: String = {
    """
      |{
      |  "type": "record",
      |  "name": "RewardsInvoice",
      |  "namespace": "com.github.fescalhao",
      |  "fields": [
      |    {"name": "CustomerCardNo","type": ["string", "null"]},
      |    {"name": "TotalPurchase","type": ["double", "null"]},
      |    {"name": "AggregatedRewards","type": ["int", "null"]}
      |  ]
      |}
      |""".stripMargin
  }
}
