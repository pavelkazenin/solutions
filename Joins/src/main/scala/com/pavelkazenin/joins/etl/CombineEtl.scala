package com.pavelkazenin.joins.etl

import com.pavelkazenin.joins.model.{Customers, Orders, Results}
import com.pavelkazenin.joins.utils.{AppContext, Constants}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object CombineEtl {

  def executeJob(spark: SparkSession, appContext: AppContext): Unit = {

    import spark.implicits._

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

    val unionDF: DataFrame = ordersDF.unionByName(customersDF, true)

    val unionPairRDD = unionDF.rdd.map( row => (row.getAs[Int]("customer_id"), row))

    val combinedRDD = unionPairRDD.combineByKey( createCombiner, mergeValue, mergeCombiners )

    val resultsCols = Results.SCHEMA.toList.map { st => st.name }

    combinedRDD.filter( filterZeros )
      .map( row => (row._1, row._2.firstName, row._2.lastName, row._2.count ) )
      .toDF(resultsCols: _*)
      .write
      .option("sep", "|")
      .option("header", "true")
      .csv(appContext.get(Constants.OUTPUT_RESULTS))

  }

  def filterZeros( row: (Int, OrdersCount)) : Boolean = {
    if ( row._2.count == 0 ) false else true
  }

  def createCombiner( row: Row): OrdersCount = {
    new OrdersCount(
      row.getAs[String]("first_name"),
      row.getAs[String]("last_name"),
      0
    )
  }

  def mergeValue( acc: OrdersCount, row: Row): OrdersCount = {
    if ( !row.isNullAt(0) ) {    // order_id
       acc.count += 1
    }
    else {
      // assign firstName, lastName
      acc.firstName = row.getAs[String]("first_name")
      acc.lastName = row.getAs[String]("last_name")
    }

    acc
  }

  def mergeCombiners ( acc1: OrdersCount, acc2: OrdersCount ): OrdersCount = {
    if (acc2.firstName != null && acc2.lastName != null) {
      acc1.firstName = acc2.firstName
      acc1.lastName = acc2.lastName
    }

    acc1.count += acc2.count

    acc1
  }

  case class OrdersCount( var firstName: String,
                          var lastName: String, var count: Int) extends Serializable {

    if ( firstName == null && lastName == null ) count = 1
  }

}
