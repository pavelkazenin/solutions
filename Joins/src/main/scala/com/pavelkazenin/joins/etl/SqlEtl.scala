package com.pavelkazenin.joins.etl

import com.pavelkazenin.joins.model.{Customers, Orders}
import com.pavelkazenin.joins.utils.{AppContext, Constants}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SqlEtl {


   def executeJob(spark: SparkSession, appContext: AppContext): Unit = {

    spark.read
      .option("sep", "|")
      .option("header", "true")
      .schema(Customers.SCHEMA)
      .csv(appContext.get(Constants.INPUT_CUSTOMERS))
      .createOrReplaceGlobalTempView("customers")

    spark.read
      .option("sep", "|")
      .option("header", "true")
      .schema(Orders.SCHEMA)
      .csv(appContext.get(Constants.INPUT_ORDERS))
      .createOrReplaceGlobalTempView("orders")

    val joinedDF = spark.sql(
    """SELECT c.customer_id, c.first_name, c.last_name, count(o.order_id) as count
           FROM global_temp.customers c, global_temp.orders o
          WHERE c.customer_id = o.customer_id
          GROUP BY c.customer_id, c.first_name, c.last_name""")


    joinedDF.write
      .option("sep", "|")
      .option("header", "true")
      .csv(appContext.get(Constants.OUTPUT_RESULTS))

  }

}
