package com.pavelkazenin.bloomfilter.stream

import com.google.common.hash.BloomFilter
import com.pavelkazenin.bloomfilter.etl.FilterDataEtl.INCR_DATA_PATH
import com.pavelkazenin.bloomfilter.guava.IntegerFunnel
import com.pavelkazenin.bloomfilter.model.Customers
import com.pavelkazenin.bloomfilter.utils.{AppContext, Constants}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

import java.io.FileInputStream

object FilterDataStream extends GenericStream with Constants {

  override def executeStream(spark: SparkSession, appContext: AppContext): Unit = {

    // Read and broadcast Bloom Filter
    val initialBF: BloomFilter[Integer] = BloomFilter.readFrom(new FileInputStream(appContext.get(FILTER_PATH)), IntegerFunnel.INSTANCE)

    val bcInitialBF = spark.sparkContext.broadcast(initialBF)

    // Read Incremental Data
    val incrementalDF = spark.readStream
      .option("sep", "|")
      .option("header", "true")
      .schema(Customers.SCHEMA)
      .csv(appContext.get(INCR_DATA_PATH))

    def filterNewOrModified(row: Row): Boolean = {
      !bcInitialBF.value.mightContain(Customers.murmurHash3(row))
    }

    val newOrModifiedDF = incrementalDF.filter(row => filterNewOrModified(row))

    val query = newOrModifiedDF.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
