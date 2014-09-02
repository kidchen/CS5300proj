package simplePR;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Counters.Counter;

import simplePR.simplePRCounter;

import simplePR.simplePRMapper;
import simplePR.simplePRReducer;

public class simplePRDriver {
	//The total number of nodes
	public static final int numOfNodes = 685230;
	
    public static void main(String[] args) throws Exception {
    	simplePRDriver driver = new simplePRDriver();
    	//run 5 passes, record each pass's residual error in array
        double[] totalResidual = new double[5];
        // run the first pass, the input file is the preprocessed data file uploaded to S3 bucket;
        // the output file is stored in S3 bucket, and used as the next pass's input.
        double residual = driver.runsimplePageRank(args[0], args[1] + "/pass"+String.valueOf(1));
    	//store the total residual error of first pass in array.
        totalResidual[0] = residual;
        //run the next 4 passes, each pass's input is the former pass's output and store the total residual error of each pass in array.
        for (int numPass = 1; numPass < 5; ++ numPass) {
        	residual = driver.runsimplePageRank(args[1] + "/pass" + String.valueOf(numPass), args[1] + "/pass" + String.valueOf(numPass + 1));
            totalResidual[numPass] = residual;
        }
        
        // Calculate the average residual error by dividing the total residual error by the total number of nodes and print them
        for (int numPass = 0; numPass < 5; ++ numPass) {
        	System.out.println("Average Residual Value: " + totalResidual[numPass] / numOfNodes);
        }
    }
    
    public double runsimplePageRank(String InputPath, String OutputPath) throws Exception { 
    	// Create a new JobConf
    	JobConf conf = new JobConf(simplePRDriver.class);
    	// Specify various job-specific parameters  
        conf.setJarByClass(simplePRDriver.class);
    
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
  
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(conf, new Path(InputPath));
        FileOutputFormat.setOutputPath(conf, new Path(OutputPath));
 
        conf.setMapperClass(simplePRMapper.class);
        conf.setReducerClass(simplePRReducer.class);
        // Submit the job, then poll for progress until the job is complete
        RunningJob job = JobClient.runJob(conf);
        
        // Use counter to get the residual
        Counters counters = job.getCounters();
        Counter myCounter = counters.findCounter(simplePRCounter.RESIDUAL_COUNTER);
        //myCounter.getValue() return the sum amplified residual value
        double residualSum = myCounter.getValue() / (1.0*10e4);
        
        return residualSum;
    }

}
