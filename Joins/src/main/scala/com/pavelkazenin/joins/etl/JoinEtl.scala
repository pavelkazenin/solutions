package com.pavelkazenin.joins.etl

import com.pavelkazenin.joins.model.{Customers, Orders}
import com.pavelkazenin.joins.utils.{AppContext, Constants}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object JoinEtl {


   def executeJob(spark: SparkSession, appContext: AppContext): Unit = {

     val customersDF: DataFrame = spark.read
      .option("sep", "|")
      .option("header", "true")
      .schema(Customers.SCHEMA)
      .csv(appContext.get(Constants.INPUT_CUSTOMERS))

     val ordersDF: DataFrame = spark.read
      .option("sep", "|")
      .option("header", "true")
      .schema(Orders.SCHEMA)
      .csv(appContext.get(Constants.INPUT_ORDERS))

     val joinedDF: DataFrame = ordersDF
      .join(customersDF, "customer_id")
      .groupBy("customer_id", "first_name", "last_name").count()

     joinedDF.write
       .option("sep", "|")
       .option("header", "true")
       .csv(appContext.get(Constants.OUTPUT_RESULTS))

  }

}
