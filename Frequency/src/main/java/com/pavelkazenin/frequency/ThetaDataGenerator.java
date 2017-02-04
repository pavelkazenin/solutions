package com.pavelkazenin.frequency;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.commons.math3.util.Precision;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class ThetaDataGenerator {

	final static double SHIFT =  Math.random()*2*Math.PI;
	final static int PRECISION = 8;
	
	public static void main(String[] args) {
		
		ThetaDataGeneratorArgs generatorArgs = new ThetaDataGeneratorArgs();
		CmdLineParser parser = new CmdLineParser(generatorArgs);
		
		try {
            parser.parseArgument(args);
		} 
		catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			System.exit(1);
		}
		
		double ft = Double.parseDouble(generatorArgs.freq);
		int samples = Integer.parseInt(generatorArgs.samples);
		int period = Integer.parseInt(generatorArgs.period);
		String signalFile = generatorArgs.inputPath;
		
		
		double x;
		double[] a = new double[samples];
		
		for ( int i=0; i<samples; i++) {
			a[i] = Precision.round(( Math.random()/1000 * period), PRECISION); // in millis
		}

		PrintWriter pw = null;
		
		try {
			pw = new PrintWriter(new File(signalFile));
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
					
		for ( int i=0; i<samples; i++) {
			x = Math.sin( 2*Math.PI * ft * a[i] + SHIFT );
			pw.printf("%."+PRECISION+"f %."+PRECISION+"f\n", a[i], x);
		}
		
		pw.close();
	}	
}
