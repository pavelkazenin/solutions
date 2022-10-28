package com.pavelkazenin.joins.etl

import com.pavelkazenin.joins.model.{Customers, Orders, Results}
import com.pavelkazenin.joins.utils.{AppContext, Constants}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.language.postfixOps

object GroupEtl {

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

    val groupedRDD = unionPairRDD.groupByKey().map( aggregateOrders )

    val resultsCols = Results.SCHEMA.toList.map { st => st.name }

    groupedRDD.filter( filterZeros )
      .map(row => (row._1, row._2.firstName, row._2.lastName, row._2.count))
      .toDF(resultsCols: _*)
      .write
      .option("sep", "|")
      .option("header", "true")
      .csv(appContext.get(Constants.OUTPUT_RESULTS))

  }

  def aggregateOrders( row: (Int, Iterable[Row]) ): (Int, OrdersCount) = {

    val customerId = row._1
    val groupedRows = row._2

    val ordersCount = new OrdersCount( null, null, 0)

    for ( groupedRow <- groupedRows ) {
      if (!groupedRow.isNullAt(0)) { // order_id
        ordersCount.count += 1
      }
      else {
        // assign firstName, lastName
        ordersCount.firstName = groupedRow.getAs[String]("first_name")
        ordersCount.lastName = groupedRow.getAs[String]("last_name")
      }

    }

    ( customerId, ordersCount )
  }

  def filterZeros( row: (Int, OrdersCount)) : Boolean = {
    if ( row._2.count == 0 ) false else true
  }

  case class OrdersCount( var firstName: String,
                          var lastName: String, var count: Int) extends Serializable

}
