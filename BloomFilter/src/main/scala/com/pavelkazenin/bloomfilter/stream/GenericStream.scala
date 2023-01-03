package com.pavelkazenin.bloomfilter.stream

import com.pavelkazenin.bloomfilter.utils.AppContext
import org.apache.spark.sql.SparkSession

trait GenericStream {

  def executeStream(spark: SparkSession, appContext: AppContext): Unit

}
