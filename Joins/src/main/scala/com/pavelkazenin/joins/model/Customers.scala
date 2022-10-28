package com.pavelkazenin.joins.model

import org.apache.spark.sql.types.{DataTypes, StructType}

object Customers {

  val TABLE = "customers"

  val SCHEMA: StructType = StructType(List(
    DataTypes.createStructField("customer_id", DataTypes.IntegerType, false),
    DataTypes.createStructField("first_name", DataTypes.StringType, false),
    DataTypes.createStructField("last_name", DataTypes.StringType, false),
    DataTypes.createStructField("street", DataTypes.StringType, false),
    DataTypes.createStructField("city", DataTypes.StringType, false),
    DataTypes.createStructField("state", DataTypes.StringType, false),
    DataTypes.createStructField("zip", DataTypes.StringType, false)
  ));

}
