package com.pavelkazenin.joins

import com.pavelkazenin.joins.etl.{CombineEtl, GroupEtl, JoinEtl, SqlEtl}
import com.pavelkazenin.joins.utils.{AppContext, Constants}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{File, IOException}

class AppTest extends AnyFlatSpec {

  val spark = SparkSession
    .builder
    .master("local[1]")
    .appName("JoinsTestETL")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate();

  val currentDir: String = System.getProperty("user.dir")
  val customers: String = currentDir + "/src/test/data/input/customers.csv"
  val orders: String = currentDir + "/src/test/data/input/orders.csv"
  val results: String = currentDir + "/src/test/data/output"

  val appContext: AppContext = new AppContext
  appContext.put(Constants.INPUT_CUSTOMERS, customers)
  appContext.put(Constants.INPUT_ORDERS, orders)
  appContext.put(Constants.OUTPUT_RESULTS, results)

  val resultsDir: File = new File(results)

  "SQL Etl" should "create result CSV file with two records" in {
    FileUtils.deleteQuietly(resultsDir)
    SqlEtl.executeJob(spark, appContext)
    val result = TestEtl.executeJob(spark, appContext)
    assert( result._1 == 2L && result._2 == 6L )
  }

  "Group Etl" should "create result CSV file with two records" in {
    FileUtils.deleteQuietly(resultsDir)
    GroupEtl.executeJob(spark, appContext)
    val result = TestEtl.executeJob(spark, appContext)
    assert(result._1 == 2L && result._2 == 6L)
  }

  "Join Etl" should "create result CSV file with two records" in {
    FileUtils.deleteQuietly(resultsDir)
    JoinEtl.executeJob(spark, appContext)
    val result = TestEtl.executeJob(spark, appContext)
    assert(result._1 == 2L && result._2 == 6L)
  }

  "Combine Etl" should "create result CSV file with two records" in {
    FileUtils.deleteQuietly(resultsDir)
    CombineEtl.executeJob(spark, appContext)
    val result = TestEtl.executeJob(spark, appContext)
    assert(result._1 == 2L && result._2 == 6L)
  }

}
