package com.pavelkazenin.bloomfilter.etl

import com.pavelkazenin.bloomfilter.model.Customers
import com.pavelkazenin.bloomfilter.utils.{AppContext, Constants}
import org.apache.spark.sql.SparkSession

object GenerateDataEtl extends GenericEtl with Constants {

  @Override
  def executeJob(spark: SparkSession, appContext: AppContext): Unit = {

    import spark.implicits._

    val numRecords = appContext.get(NUM_RECORDS).toInt
    val initIntSeq = scala.Seq.range(1, numRecords)
    val incrIntSeq = scala.Seq.range(numRecords / 2, numRecords + numRecords / 2)

    val initCustSeq = initIntSeq.map { customerId => generateCustomerRecord(customerId) }
    val incrCustSeq = incrIntSeq.map { customerId => generateCustomerRecord(customerId) }

    val customersCols = Customers.SCHEMA.toList.map { st => st.name }

    val initCustDF = spark.sparkContext
      .parallelize(initCustSeq, appContext.get(NUM_PARTITIONS).toInt).toDF(customersCols: _*)

    val incrCustDF = spark.sparkContext
      .parallelize(incrCustSeq, appContext.get(NUM_PARTITIONS).toInt).toDF(customersCols: _*)

    initCustDF.write
      .option("sep", "|")
      .option("header", "true")
      .csv(appContext.get(INIT_DATA_PATH))

    incrCustDF.write
      .option("sep", "|")
      .option("header", "true")
      .csv(appContext.get(INCR_DATA_PATH))

  }

   private def generateCustomerRecord(customerId: Int) = {
    (customerId, s"FirstName${customerId}", s"LastName${customerId}",
      s"Street Address ${customerId}", s"City${customerId}", s"State${customerId}", s"Zip${customerId}")
  }

}
