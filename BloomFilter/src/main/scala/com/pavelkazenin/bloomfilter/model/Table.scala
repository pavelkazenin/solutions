package com.pavelkazenin.bloomfilter.model

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

trait Table {

  val SCHEMA: StructType
  val NAME: String
  def murmurHash3( row: Row ): Integer
}
