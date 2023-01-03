package com.pavelkazenin.bloomfilter

import com.pavelkazenin.bloomfilter.etl.{BuildFilterEtl, FilterDataEtl, GenerateDataEtl}
import com.pavelkazenin.bloomfilter.stream.FilterDataStream
import com.pavelkazenin.bloomfilter.utils.AppContext
import com.pavelkazenin.bloomfilter.utils.Constants
import org.apache.spark.sql.SparkSession
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}

import java.lang.System.exit
import scala.collection.JavaConverters

object JobArgs {

  @Option(name = "-master", required = false, usage = "spark master")
  var master: String = "local[*]"

  @Option (name = "-initdata", required = false, usage = "initial data path")
  var initdata: String = null

  @Option (name = "-incrdata", required = false, usage = "incremental data path")
  var incrdata: String = null

  @Option (name = "-filter", required = false, usage = "bloom filter file")
  var filter: String = "/tmp/"+scala.util.Random.alphanumeric.take(6).mkString

  @Option(name = "-fpp", required = false, usage = "bloom filter fpp")
  var fpp: String = "0.01" // false positive probability 1% by default

  @Option(name = "-mode", required = true, usage = "application mode")
  var mode: String = "filter"

  @Option (name = "-partitions", required = false, usage = "number of partitions")
  var numPartitions = "10"

  @Option(name = "-records", required = false, usage = "number of records")
  var numRecords = "10000"

}

object JobRunner extends Constants {

  def main(args: Array[String]): Unit = {
    val parser = new CmdLineParser(JobArgs)

    try {
      parser.parseArgument(JavaConverters.asJavaCollection(args))
      println(s"Spark Master:         ${JobArgs.master}")
      println(s"InitialData Path:     ${JobArgs.initdata}")
      println(s"IncrementalData Path: ${JobArgs.incrdata}")
      println(s"Bloom Filter File:    ${JobArgs.filter}")
      println(s"Bloom Filter FPP:     ${JobArgs.fpp}")
      println(s"Runner Mode:          ${JobArgs.mode}")
      println(s"Number of Records:    ${JobArgs.numRecords}")
      println(s"Number of Partitions: ${JobArgs.numPartitions}")
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
      .appName("BloomFilterETL")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();

    spark.sparkContext.setLogLevel("ERROR")

    val appContext = new AppContext

    appContext.put(INIT_DATA_PATH, JobArgs.initdata)
    appContext.put(INCR_DATA_PATH, JobArgs.incrdata)
    appContext.put(FILTER_PATH, JobArgs.filter)
    appContext.put(FILTER_FPP, JobArgs.fpp)
    appContext.put(NUM_PARTITIONS, JobArgs.numPartitions)
    appContext.put(NUM_RECORDS, JobArgs.numRecords)

    try {
      JobArgs.mode match {
        case MODE_GENERATE => GenerateDataEtl.executeJob(spark, appContext)
        case MODE_BUILD => BuildFilterEtl.executeJob(spark, appContext)
        case MODE_FILTER => FilterDataEtl.executeJob(spark, appContext)
        case MODE_STREAM => FilterDataStream.executeStream(spark, appContext)
        case _ => {
          println("Usage:")
          parser.printUsage(System.out)
        }
      }
    }
    finally {
      spark.stop()
    }
  }
}
