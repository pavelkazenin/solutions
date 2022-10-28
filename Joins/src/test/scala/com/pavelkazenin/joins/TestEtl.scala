package com.pavelkazenin.joins

import com.pavelkazenin.joins.model.{Customers, Orders, Results}
import com.pavelkazenin.joins.utils.{AppContext, Constants}
import org.apache.spark.sql.SparkSession

object TestEtl {


  def executeJob(spark: SparkSession, appContext: AppContext): Tuple2[Long, Long] = {

    spark.read
      .option("sep", "|")
      .option("header", "true")
      .schema(Results.SCHEMA)
      .csv(appContext.get(Constants.OUTPUT_RESULTS))
      .createOrReplaceGlobalTempView("results")

    val aggrDF = spark.sql(
    """SELECT count(*) as count, sum(count) as sum
           FROM global_temp.results""")

    val aggrList = aggrDF.collectAsList()

    if ( aggrList.isEmpty ) {
      ( 0L, 0L )
    }
    else {
      ( aggrList.get(0).getLong(0), aggrList.get(0).getLong(1) )
    }

  }

}
