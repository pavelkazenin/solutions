package com.pavelkazenin.joins.etl

import com.pavelkazenin.joins.model.{Customers, Orders}
import com.pavelkazenin.joins.utils.{AppContext, Constants}
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.util.Random

object GenerateEtl {


   def executeJob(spark: SparkSession, appContext: AppContext): Unit = {

     import spark.implicits._

     val integersSeq = scala.Seq.fill(appContext.get(Constants.NUM_RECORDS).toInt){scala.util.Random.nextInt(Constants.MAX_CUSTOMER_ID)}
     val customersSeq = integersSeq.map{ customerId => generateCustomerRecord(customerId) }
     val ordersSeq1 = Random.shuffle(integersSeq).map{ customerId => generateOrderRecord(customerId, 1) }
     val ordersSeq2 = Random.shuffle(integersSeq).map{ customerId => generateOrderRecord(customerId, 2) }
     val ordersSeq3 = Random.shuffle(integersSeq).map{ customerId => generateOrderRecord(customerId, 3) }
     val ordersSeq = ordersSeq2 ++ ordersSeq3 ++ ordersSeq1

     val customersCols = Customers.SCHEMA.toList.map{ st => st.name }
     val customersDF = spark.sparkContext.parallelize(customersSeq, appContext.get(Constants.NUM_PARTITIONS).toInt)
       .toDF(customersCols: _*)

     val ordersCols = Orders.SCHEMA.toList.map{ st => st.name }
     val ordersDF = spark.sparkContext.parallelize(ordersSeq, appContext.get(Constants.NUM_PARTITIONS).toInt)
       .toDF(ordersCols: _*)

     customersDF.write
       .option("sep", "|")
       .option("header", "true")
       .csv(appContext.get(Constants.INPUT_CUSTOMERS))

     ordersDF.write
       .option("sep", "|")
       .option("header", "true")
       .csv(appContext.get(Constants.INPUT_ORDERS))

   }

  def generateCustomerRecord( customerId: Int ) = {
     (customerId, s"FirstName${customerId}", s"LastName${customerId}",
       s"Street Address ${customerId}", s"City${customerId}", s"State${customerId}", s"Zip${customerId}")
  }

  def generateOrderRecord( customerId: Int, seqNum: Int ) = {
     (customerId*100 + seqNum, customerId, 10000 + seqNum, 10 + seqNum, Timestamp.valueOf(LocalDateTime.now))
  }

}
