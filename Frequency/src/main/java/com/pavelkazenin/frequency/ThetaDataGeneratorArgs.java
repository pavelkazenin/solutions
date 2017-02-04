package com.pavelkazenin.frequency;

import org.kohsuke.args4j.Option;

public class ThetaDataGeneratorArgs {

    @Option(name= "-in", required=false, usage = "path to generated signal data file" )
    public String inputPath = "/tmp/thetaData";
      
    @Option(name= "-frequency", required=false, usage = "signal frequency, cycles per second")
    public String freq = "100000.0";

    @Option(name = "-period", required=false, usage = "period of signal sampling, milliseconds")
    public String period = "10";
    
    @Option(name= "-samples", required=false, usage = "number of signal samples")
    public String samples = "10";

}
