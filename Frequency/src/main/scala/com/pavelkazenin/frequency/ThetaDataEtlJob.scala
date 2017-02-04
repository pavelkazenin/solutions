package com.pavelkazenin.frequency

import org.apache.spark.{SparkConf, SparkContext, SparkJobInfo}
import org.apache.spark.rdd.RDD
import scala.collection.{mutable}

import org.apache.commons.math3.util.Precision

object ThetaDataEtlJob extends ThetaDataConstants {
  
    def executeJob( sc: SparkContext, tdc: ThetaDataContext ): ThetaResult = {
      
        val inputFile = tdc.get(INPUT_PROP_NAME)
        
        // broadcast common variables to all partitions
        val bc_ft = sc.broadcast(tdc.get(CARRIER_PROP_NAME).toDouble)
        val bc_fh = sc.broadcast(tdc.get(BAND_PROP_NAME).toDouble)
        val bc_K  = sc.broadcast(tdc.get(PROBES_PROP_NAME).toInt)
        val bc_B  = sc.broadcast(tdc.get(BINS_PROP_NAME).toInt)
        
        // run the job
        sc
        
        // read input
        .textFile(inputFile)
             
        // RDD[String={timestamp, signal}] => RDD[ThetaRecord], K-fold increase of data set size
        .flatMap( line => parseInputLine( line, bc_ft.value, bc_fh.value, bc_K.value ) )
        
        // aggregate by frequency
        .aggregateByKey( new ThetaRecordAggr(bc_B.value) )( (out, in) => {out.addEntry(in); out}, (out, in) => {out.merge(in); out} ) 

        // pass only records with maxCounter >= number of samples
        .filter( rec => { rec._2.maxCounter >= rec._2.size } )
                
        // aggregate all frequencies, ideally should be one record per frequency
        .aggregate( new ThetaResult() )( (out, in) => {out.addEntry(in); out}, (out, in) => {out.merge(in); out} )
        
        // average all frequencies
        .avg()  
    }
    
    def getTheta1( x: Double, y: Double ): Double =  {
		    return Math.abs( y * Math.sqrt( 1 - x * x ) + x * Math.sqrt( 1 - y * y ) );
	  }
    
    def getTheta2( x: Double, y: Double ): Double =  {
		    return Math.abs( y * Math.sqrt( 1 - x * x ) - x * Math.sqrt( 1 - y * y ) );
	  }

    def parseInputLine( line: String, ft: Double, fh: Double, K: Int ) = {

        val list = mutable.ListBuffer.empty[Tuple2[Double,ThetaRecord]]
        
        val tokens = line.split("[,\\s\\r\\n\\t]+")
        val t = tokens(0).toDouble
        val x = tokens(1).toDouble
        var fk = 0.0
        var y = 0.0
        
        for ( k <- 1 to K ) {
           // left band
           fk = ft - fh * k / K
           y = Math.sin(2*Math.PI*fk*t)
           list += new Tuple2( fk, new ThetaRecord( fk, t, getTheta1( x, y ), getTheta2( x, y ) ) )
           
           // right band
           fk = ft + fh * k / K
           y = Math.sin(2*Math.PI*fk*t)
           list += new Tuple2( fk, new ThetaRecord( fk, t, getTheta1( x, y ), getTheta2( x, y ) ) )
        }
        
        // center
        fk = ft
        y = Math.sin(2*Math.PI*fk*t)
        list += new Tuple2( fk, new ThetaRecord( fk, t, getTheta1( x, y ), getTheta2( x, y ) ) )
        
        list.iterator  
    }
    
    case class ThetaRecordAggr( bins: Int ) extends Serializable {

       val bin = new Array[Int](bins)  // bins

       var maxCounter: Int = 0
       var tmin = Double.MaxValue
       var tmax = 0.0
       var size = 0

       def addEntry( entry: ThetaRecord ) = {
         
           var i = 0

           i = ( entry.theta1 * bins  ).toInt
           if ( i == bins ) i = bins-1
           bin(i) += 1
           maxCounter = Math.max(maxCounter, bin(i));
           
           i = ( entry.theta2 * bins  ).toInt
           if ( i == bins ) i = bins-1
           bin(i) += 1
           maxCounter = Math.max(maxCounter, bin(i));
           
           size += 1
           tmin = Math.min(tmin, entry.t)
           tmax = Math.max(tmax, entry.t)
       }
       
       def merge( that: ThetaRecordAggr ) = {
           for ( i <- 0 to bins-1 ) {
               bin(i) += that.bin(i)
               maxCounter = Math.max(maxCounter, bin(i))
           }
           
           size += that.size
           tmin = Math.min(tmin, that.tmin)
           tmax = Math.max(tmax, that.tmax)
       }      
    }
    
    case class ThetaResult() extends Serializable {

       var fk = 0.0
       var ta = 0.0
       var counter = 0
       
       def addEntry( entry: (Double, ThetaRecordAggr) ) = {
          fk += entry._1
          ta += (entry._2.tmin+entry._2.tmax)/2
          counter += 1
       }
       
       def merge( that: ThetaResult ) = {
          fk += that.fk
          ta += that.ta
          counter += that.counter
       }
       
       def avg() = { 
         if ( counter > 0 ) { fk /= counter; ta /= counter }
         this
       }
    }
    
    case class ThetaRecord( fk: Double, t: Double, theta1: Double, theta2: Double )
}