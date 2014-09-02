package simplePR;

import java.io.IOException;
import java.util.Iterator;
import java.lang.Math;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import simplePR.simplePRCounter;

public class simplePRReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	private static final double dampingFactor = 0.85F;
    
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		//key - the key.
		//values - the list of values to reduce.
		//output - to collect keys and combined values.
		//reporter - facility to report progress.
		String value;
    	String dstNodeIDList = "";
    	double sumPageRank = 0;
    	double oldPageRank = 0;
    	
        while (values.hasNext()) {
        	value = values.next().toString();
        	//  "list:" denote links
        	if (value.startsWith("list:")) {
        		dstNodeIDList = "\t" + value.substring(5);
        		continue;
        	}
        	// "pr:" denote node with old page rank
        	if (value.startsWith("pr:")) {
        		oldPageRank = Double.parseDouble(value.substring(3));
        		continue;
        	}
        	// Calculate new PR.
        	double pageRank = Double.parseDouble(value);
        	sumPageRank += pageRank;
        }
        //count the newPageRank for this node
        double newPageRank = dampingFactor *  sumPageRank + (1 - dampingFactor);
        
       	double sumresidualValue = Math.abs(newPageRank - oldPageRank) / newPageRank;
       //convert the sumresidualValue to long by amplify it by 10^4.
        long sumamplifiedValue = (long)Math.floor(sumresidualValue * 10e4);
        reporter.incrCounter(simplePRCounter.RESIDUAL_COUNTER, sumamplifiedValue);
        output.collect(key, new Text(newPageRank + dstNodeIDList));
    }
}
