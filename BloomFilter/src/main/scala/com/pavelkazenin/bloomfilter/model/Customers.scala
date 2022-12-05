package com.pavelkazenin.bloomfilter.model

import org.apache.commons.codec.digest.MurmurHash3
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.security.MessageDigest

object Customers extends Table {

  @Override
  val NAME: String = "customers"

  @Override
  val SCHEMA: StructType = new StructType()
    .add("customer_id", DataTypes.IntegerType, false)
    .add("first_name", DataTypes.StringType, false)
    .add("last_name", DataTypes.StringType, false)
    .add("street", DataTypes.StringType, false)
    .add("city", DataTypes.StringType, false)
    .add("state", DataTypes.StringType, false)
    .add("zip", DataTypes.StringType, false)

  /*
   * Calculate MurmurHash3 code from Customer record
   */
  @Override
  def murmurHash3( row: Row ): Integer = {

    val md: MessageDigest = MessageDigest.getInstance("MD5")

    val md5 =
      md.digest(row.getInt(0).toString.getBytes) ++
      md.digest(row.getString(1).getBytes) ++
      md.digest(row.getString(2).getBytes) ++
      md.digest(row.getString(3).getBytes) ++
      md.digest(row.getString(4).getBytes) ++
      md.digest(row.getString(5).getBytes) ++
      md.digest(row.getString(6).getBytes)

    Integer.valueOf(MurmurHash3.hash32x86( md5 ))
  }
}
