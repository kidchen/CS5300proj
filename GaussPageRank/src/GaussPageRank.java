import java.util.*;
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

public class GaussPageRank {    
	public static final int NumberOfNodes = 685230;
	
    public static void main(String[] args) throws Exception {
    	GaussPageRank rank = new GaussPageRank();
    	//record each pass's residual error in array
    	ArrayList<Double> residualArray = new ArrayList<Double>();
    	ArrayList<Double> residualAndHighestNodePR = new ArrayList<Double>();
    
    	int numPass = 0;
    	System.out.println("MapReduce Pass " + numPass + ":");
    	
    	// run the first pass, the input file is the preprocessed data file uploaded to S3 bucket;
        // the output file is stored in S3 bucket, and used as the next pass's input.
    	residualAndHighestNodePR = rank.runPageRankCalculation(args[0], args[1] + "/pass" + String.valueOf(numPass));
        double averageResidual  = residualAndHighestNodePR.get(0);
    	residualArray.add(averageResidual);
    	
    	//run passes until the average residual error is below 0.001
    	// each pass's input is the former pass's output and store the total residual error of each pass in array.
    	while (averageResidual > 0.001) {
    		++ numPass;
    		System.out.println("MapReduce Pass " + numPass + ":");
    		residualAndHighestNodePR = rank.runPageRankCalculation(args[1] + "/pass" + String.valueOf(numPass - 1), args[1] + "/pass" + String.valueOf(numPass));
    		averageResidual  = residualAndHighestNodePR.get(0);
        	residualArray.add(averageResidual);
        }
        
    	//print out info(total number of passes, average residual error for each pass and the highest page rank node in block.
    	System.out.println("Total Number of Passes: " + (numPass + 1));
        // Print residuals:
        for (int i = 0; i < residualArray.size(); ++ i) {
        	String residualStr = new String(String.format("Average Residual Error for Pass %d: %.4g", i, residualArray.get(i)));
        	System.out.println(residualStr);
        }
        
        // Print the PR values of the highest numbered node in each block from the last pass
        for (int i = 1; i < residualAndHighestNodePR.size(); ++ i) {
        	String highestStr = new String(String.format("The page rank of highest numbered Node in Block %d is %.4g", i-1, residualAndHighestNodePR.get(i)));
        	System.out.println(highestStr);
        }
    }
    
    public ArrayList<Double> runPageRankCalculation(String InputPath, String OutputPath) throws Exception {   
    	// Create a new JobConf
    	JobConf conf = new JobConf(GaussPageRank.class);
    	// Specify various job-specific parameters 
        conf.setJarByClass(GaussPageRank.class);
    	 
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
 
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(conf, new Path(InputPath));
        FileOutputFormat.setOutputPath(conf, new Path(OutputPath));
 
        conf.setMapperClass(BlockedPageRankCalculateMap.class);
        conf.setReducerClass(BlockedPageRankCalculateReduce.class);
        
        // Submit the job, then poll for progress until the job is complete
        RunningJob job = JobClient.runJob(conf);
        
        // Use counter to get the global average residual, iteration number of each pass and highest pr node in each PR
        Counters counters = job.getCounters();
        Counter c = counters.findCounter(MyCounter.GLOBAL_RESIDUAL_COUNTER);
        double totalresidual = c.getValue() / (1.0*10e6);
        
        c = counters.findCounter(MyCounter.LOCAL_ITERATION_NUM_COUNTER);
        long iter_sum = c.getValue();
        c = counters.findCounter(MyCounter.BLOCK_COUNTER);
        long block_num = c.getValue();
        
        long avg_iter_per_block = Math.round(iter_sum * 1.0 / block_num);
  
        System.out.println("Average number of iterations per Block: " + avg_iter_per_block);
        
        ArrayList<Double> residualAndHighestNodePR = new ArrayList<Double> ();
        
        // Caculate the average residual error by dividing the total residual error by the total number of nodes and print them
        double averageResidual = totalresidual / NumberOfNodes;
        String residualStr = new String(String.format("Global Average Residual Error: %.4g", averageResidual));
        // add the global average residual error and the highest PR node to arraylist and return the arraylist
        residualAndHighestNodePR.add(averageResidual);
        System.out.println(residualStr);
        // find the PR value of the highest numbered node in each block and restore it in array residualAndHighestNodePR
        for (int i = 0; i < block_num; ++ i) {
        	c = counters.findCounter(HighestRankCounter.values()[i]);
        	double totalhighestValue = c.getValue() / (1.0*10e6);
        	double highestValue = totalhighestValue / NumberOfNodes;
        	residualAndHighestNodePR.add(highestValue);
        }
        
        return residualAndHighestNodePR;
    }
    
    
}
