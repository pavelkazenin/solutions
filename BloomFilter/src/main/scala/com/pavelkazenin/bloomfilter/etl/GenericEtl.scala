package com.pavelkazenin.bloomfilter.etl

import com.pavelkazenin.bloomfilter.utils.AppContext
import org.apache.spark.sql.SparkSession

trait GenericEtl {

  def executeJob(spark: SparkSession, appContext: AppContext): Unit

}
