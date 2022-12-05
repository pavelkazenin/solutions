package com.pavelkazenin.bloomfilter.etl

import com.google.common.hash.BloomFilter
import com.pavelkazenin.bloomfilter.etl.BuildFilterEtl.FILTER_PATH
import com.pavelkazenin.bloomfilter.guava.IntegerFunnel
import com.pavelkazenin.bloomfilter.model.Customers
import com.pavelkazenin.bloomfilter.utils.{AppContext, Constants}
import org.apache.spark.sql.{Row, SparkSession}

import java.io.FileInputStream

object FilterDataEtl extends GenericEtl with Constants {

  @Override
  def executeJob(spark: SparkSession, appContext: AppContext): Unit = {

    // Read and broadcast Bloom Filter
    val initialBF: BloomFilter[Integer] = BloomFilter.readFrom(new FileInputStream(appContext.get(FILTER_PATH)), IntegerFunnel.INSTANCE)

    val bcInitialBF = spark.sparkContext.broadcast(initialBF)

    // Read Incremental Data
    val incrementalDF = spark.read
      .option("sep", "|")
      .option("header", "true")
      .schema(Customers.SCHEMA)
      .csv(appContext.get(INCR_DATA_PATH))

    def filterNewOrModified(row: Row): Boolean = {
      !bcInitialBF.value.mightContain(Customers.murmurHash3(row))
    }

    val newOrModifiedDF = incrementalDF.filter( row => filterNewOrModified(row) )

    val newOrModifiedCountEst = newOrModifiedDF.count()
    val totalCountExact = appContext.get(NUM_RECORDS).toInt
    val newOrModifiedCountExact = totalCountExact / 2
    val fppEst = Math.abs(newOrModifiedCountEst - newOrModifiedCountExact) * 1.0 / totalCountExact
    val fppExact = appContext.get(FILTER_FPP).toDouble * 1.0

    println(f"==> Filter capacity ${totalCountExact}%d, filter fpp ${fppExact}%2.6f, measured fpp ${fppEst}%2.6f")
  }
}
