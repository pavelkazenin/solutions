package com.pavelkazenin.frequency

import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}
import scala.collection.JavaConversions._
import java.io._

object ThetaDataEtlArgs  {
  
    @Option(name= "-in", required=false, usage = "path to input file" )
    var inputPath : String = "/tmp/thetaData"
  
    @Option(name= "-out", required=false, usage = "path to output file" )
    var outputPath : String = "/tmp/thetaResult"
  
    @Option(name= "-master", required=false, usage = "master URL" )
    var master : String = "local[1]"
    
    @Option(name= "-carrier", required=false, usage = "carrier")
    var carrier : String = "100000.0"

    @Option(name= "-bandwidth", required=false, usage = "bandwith")
    var band : String = "100.0"

    @Option(name= "-probes", required=false, usage = "number of probes")
    var probes : String = "200"

    @Option(name= "-bins", required=false, usage = "number of bins")
    var bins : String = "50"
}

object ThetaDataEtlRunner extends ThetaDataConstants {
  
   def main(args: Array[String]) {

      val parser = new CmdLineParser(ThetaDataEtlArgs)
      
      try {
         parser.parseArgument(args.toList) 
      } 
      catch {
         case e : CmdLineException =>
            print( s"Error: ${e.getMessage}\nUsage:\n")
            parser.printUsage(System.out)
            System.exit(1)
      }
      
      val conf = new SparkConf()
                    .setAppName("ThetaData Etl")
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      
      conf.setMaster( ThetaDataEtlArgs.master )
  
      val sc = new SparkContext(conf)
      
      val tdc = new ThetaDataContext()
      
      tdc.put( INPUT_PROP_NAME, ThetaDataEtlArgs.inputPath )
      tdc.put( OUTPUT_PROP_NAME, ThetaDataEtlArgs.outputPath )
      tdc.put( CARRIER_PROP_NAME, ThetaDataEtlArgs.carrier )
      tdc.put( BAND_PROP_NAME, ThetaDataEtlArgs.band )
      tdc.put( PROBES_PROP_NAME, ThetaDataEtlArgs.probes )
      tdc.put( BINS_PROP_NAME, ThetaDataEtlArgs.bins )
      
      val pw = new PrintWriter(new File(ThetaDataEtlArgs.outputPath ))
      
      try {
         pw.println("Input parameters:")
         pw.println("  inputFile = " + ThetaDataEtlArgs.inputPath)
         pw.println("  carrier   = " + ThetaDataEtlArgs.carrier)
         pw.println("  bandwidth = " + ThetaDataEtlArgs.band)
         pw.println("  probes    = " + ThetaDataEtlArgs.probes)
         pw.println("  bins      = " + ThetaDataEtlArgs.bins)
         
         val thetaResult = ThetaDataEtlJob.executeJob( sc, tdc )   
         
         pw.println("Output results:")
         pw.println("FK = " + thetaResult.fk +", TA = " + thetaResult.ta)
      }
      finally {
         sc.stop()
         pw.close
      }
   }
}