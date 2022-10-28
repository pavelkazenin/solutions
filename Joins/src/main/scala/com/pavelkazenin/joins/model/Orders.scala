package com.pavelkazenin.joins.model

import org.apache.spark.sql.types.{DataTypes, StructType}

object Orders {

  val TABLE = "orders"

  val SCHEMA: StructType = StructType(List(
    DataTypes.createStructField("order_id", DataTypes.IntegerType, false),
    DataTypes.createStructField("customer_id", DataTypes.IntegerType, false),
    DataTypes.createStructField("product_id", DataTypes.IntegerType, false),
    DataTypes.createStructField("quantity", DataTypes.IntegerType, false),
    DataTypes.createStructField("order_date", DataTypes.TimestampType, false)
  ));

}
