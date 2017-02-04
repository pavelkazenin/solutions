package com.pavelkazenin.frequency

import org.junit.Assert.assertTrue
import org.junit.Test

import org.apache.spark.{SparkConf, SparkContext}

class ThetaDataEtlTest extends ThetaDataConstants {
 
  val sc = new SparkContext( 
              new SparkConf()
                .setAppName("ThetaData Etl Test")
                .setMaster("local[1]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"
              )
           )
  
  val tdc = new ThetaDataContext()

  @Test
  def test_10001_100 = {
    
    val currentDir: String = System.getProperty("user.dir")
    val signalFile: String = currentDir + "/testData/input/test-10001-100"
    
      val tdc = new ThetaDataContext()
      
      tdc.put( INPUT_PROP_NAME, signalFile )
      tdc.put( CARRIER_PROP_NAME, "10000.0" )
      tdc.put( BAND_PROP_NAME, "10.0" )
      tdc.put( PROBES_PROP_NAME, "200" )
      tdc.put( BINS_PROP_NAME, "50" )

      val result = ThetaDataEtlJob.executeJob(sc, tdc)
      sc.stop
      
      assertTrue( "Signal frequency = 10001.0, calculated frequency = " + result.fk, result.fk == 10001.0 )
  }
  
  @Test
  def test_100010_10 = {
    
    val currentDir: String = System.getProperty("user.dir")
    val signalFile: String = currentDir + "/testData/input/test-100010-10"
    
              
      val tdc = new ThetaDataContext()
      
      tdc.put( INPUT_PROP_NAME, signalFile )
      tdc.put( CARRIER_PROP_NAME, "100000.0" )
      tdc.put( BAND_PROP_NAME, "100.0" )
      tdc.put( PROBES_PROP_NAME, "200" )
      tdc.put( BINS_PROP_NAME, "50" )

    val result = ThetaDataEtlJob.executeJob(sc, tdc)
    sc.stop
    
    assertTrue( "Signal frequency = 100010.0, calculated frequency = " + result.fk, result.fk == 100010.0 )
  }
  
  @Test
  def test_100021_10 = {
    
    val currentDir: String = System.getProperty("user.dir")
    val signalFile: String = currentDir + "/testData/input/test-100021-10"
    
              
      val tdc = new ThetaDataContext()
      
      tdc.put( INPUT_PROP_NAME, signalFile )
      tdc.put( CARRIER_PROP_NAME, "100000.0" )
      tdc.put( BAND_PROP_NAME, "100.0" )
      tdc.put( PROBES_PROP_NAME, "200" )
      tdc.put( BINS_PROP_NAME, "50" )

    val result = ThetaDataEtlJob.executeJob(sc, tdc)
    sc.stop
    
    assertTrue( "Signal frequency = 100021.0, calculated frequency = " + result.fk, result.fk == 100021.0 )
  }

  @Test
  def test_99995_10 = {
    
    val currentDir: String = System.getProperty("user.dir")
    val signalFile: String = currentDir + "/testData/input/test-99995-10"
    
              
      val tdc = new ThetaDataContext()
      
      tdc.put( INPUT_PROP_NAME, signalFile )
      tdc.put( CARRIER_PROP_NAME, "100000.0" )
      tdc.put( BAND_PROP_NAME, "100.0" )
      tdc.put( PROBES_PROP_NAME, "200" )
      tdc.put( BINS_PROP_NAME, "50" )

    val result = ThetaDataEtlJob.executeJob(sc, tdc)
    sc.stop
    
    assertTrue( "Signal frequency = 99995.0, calculated frequency = " + result.fk, result.fk == 99995.0 )
  }
  
}