package com.pavelkazenin.joins

import com.pavelkazenin.joins.etl.{CombineEtl, GenerateEtl, GroupEtl, JoinEtl, SqlEtl}
import com.pavelkazenin.joins.utils.{AppContext, Constants}
import org.apache.spark.sql.SparkSession
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}

import java.lang.System.exit
import scala.collection.JavaConverters

object JobArgs {

  @Option(name = "-master", required = false, usage = "spark master")
  var master: String = "local[1]"

  @Option (name = "-customers", required = true, usage = "customers file")
  var customers: String = null

  @Option (name = "-orders", required = true, usage = "orders file")
  var orders: String = null

  @Option (name = "-results", required = false, usage = "results file")
  var results: String = "/tmp/"+scala.util.Random.alphanumeric.take(6).mkString

  @Option(name = "-mode", required = true, usage = "etl mode")
  var mode: String = "sql"

  @Option (name = "-partitions", required = false, usage = "number of partitions")
  var numPartitions = "10"

  @Option(name = "-records", required = false, usage = "number of records")
  var numRecords = "10000"

}

object JobRunner {

  def main(args: Array[String]): Unit = {
    val parser = new CmdLineParser(JobArgs)
    try {
      parser.parseArgument(JavaConverters.asJavaCollection(args))
      println(s"Spark Master:  ${JobArgs.master}")
      println(s"Customer File: ${JobArgs.customers}")
      println(s"Orders File:   ${JobArgs.orders}")
      println(s"Results File:  ${JobArgs.results}")
      println(s"Runner Mode:   ${JobArgs.mode}")
      println(s"Records:       ${JobArgs.numRecords}")
      println(s"Partitions:    ${JobArgs.numPartitions}")
    }
    catch {
      case e: CmdLineException =>
        println(s"Error: ${e.getMessage}")
        println("Usage:")
        parser.printUsage(System.out)
        exit(1)
    }


    val spark = SparkSession
      .builder
      .master(JobArgs.master)
      .appName("JoinsETL")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();

    val appContext = new AppContext

    appContext.put(Constants.INPUT_CUSTOMERS, JobArgs.customers)
    appContext.put(Constants.INPUT_ORDERS, JobArgs.orders)
    appContext.put(Constants.OUTPUT_RESULTS, JobArgs.results)
    appContext.put(Constants.NUM_PARTITIONS, JobArgs.numPartitions)
    appContext.put(Constants.NUM_RECORDS, JobArgs.numRecords)

    try {
      if (JobArgs.mode.equalsIgnoreCase(Constants.MODE_COMBINE)) {
        CombineEtl.executeJob(spark, appContext)
      }
      else if (JobArgs.mode.equalsIgnoreCase(Constants.MODE_JOIN)) {
        JoinEtl.executeJob(spark, appContext)
      }
      else if (JobArgs.mode.equalsIgnoreCase(Constants.MODE_GROUP)) {
        GroupEtl.executeJob(spark, appContext)
      }
      else if (JobArgs.mode.equalsIgnoreCase(Constants.MODE_SQL)) {
        SqlEtl.executeJob(spark, appContext)
      }
      else if (JobArgs.mode.equalsIgnoreCase(Constants.MODE_GENERATE)) {
        GenerateEtl.executeJob(spark, appContext)
      }
      else {
        println("Usage:")
        parser.printUsage(System.out)
      }
    }
    finally {
      spark.stop()
    }
  }
}
