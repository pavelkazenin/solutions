package com.pavelkazenin.joins.model

import org.apache.spark.sql.types.{DataTypes, StructType}

object Results {

  val TABLE = "results"

  val SCHEMA: StructType = StructType(List(
    DataTypes.createStructField("customer_id", DataTypes.IntegerType, false),
    DataTypes.createStructField("first_name", DataTypes.StringType, false),
    DataTypes.createStructField("last_name", DataTypes.StringType, false),
    DataTypes.createStructField("count", DataTypes.IntegerType, false)
  ));

}
