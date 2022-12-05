package com.pavelkazenin.bloomfilter.etl

import com.google.common.hash.BloomFilter
import com.pavelkazenin.bloomfilter.guava.IntegerFunnel
import com.pavelkazenin.bloomfilter.model.Customers
import com.pavelkazenin.bloomfilter.utils.{AppContext, Constants}
import org.apache.spark.sql.SparkSession

import java.io.FileOutputStream

object BuildFilterEtl extends GenericEtl with Constants {

  @Override
  def executeJob(spark: SparkSession, appContext: AppContext): Unit = {

    def bfCreate: BloomFilter[Integer] = {
      BloomFilter.create[Integer](IntegerFunnel.INSTANCE, appContext.get(NUM_RECORDS).toInt, appContext.get(FILTER_FPP).toDouble)
    }

    def bfAdd(bf: BloomFilter[Integer], murmurHash3: Integer): BloomFilter[Integer] = {
      bf.put(murmurHash3)
      bf
    }

    def bfMerge(bf1: BloomFilter[Integer], bf2: BloomFilter[Integer]): BloomFilter[Integer] = {
      bf1.putAll(bf2)
      bf1
    }

    // Read Initial Data and build BloomFilter
    val initialDF = spark.read
      .option("sep", "|")
      .option("header", "true")
      .schema( Customers.SCHEMA)
      .csv(appContext.get(INIT_DATA_PATH))

    val initialRDD = initialDF.rdd.map( row => Customers.murmurHash3(row) )

    val initialBF = initialRDD.aggregate[BloomFilter[Integer]](bfCreate)(bfAdd, bfMerge)

    // Persist Bloom Filter
    initialBF.writeTo(new FileOutputStream(appContext.get(FILTER_PATH)))

  }
  
}
