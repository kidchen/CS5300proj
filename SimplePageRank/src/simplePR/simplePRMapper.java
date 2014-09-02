package simplePR;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class simplePRMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{    
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		//key - the input key.
		//value - the input value.
		//output - collects mapped keys and values.
		//reporter - facility to report progress.
		// Parse input to find: page u
		//find the first place where a tab occur,get the index
    	int srcNodeID_index = value.find("\t");
    	//get the first element from value, that is the srcNodeID
    	String srcNodeID = Text.decode(value.getBytes(), 0, srcNodeID_index);
    	
    	// Parse input to find: the page rank of src node
    	int pr_index = value.find("\t", srcNodeID_index + 1);
    	Double pr = Double.parseDouble(Text.decode(value.getBytes(), srcNodeID_index + 1, pr_index - (srcNodeID_index+1)));
    	
    	String dstNodeIDList = "";
    	//if out_degree > 0, output<dstNodeID, srcNodePR/srcNodeDeg> 
    	//used in reducer to calculate the new pr for each node.
    	if (value.getLength() > (pr_index + 1)) {
    		dstNodeIDList = Text.decode(value.getBytes(), pr_index + 1, value.getLength() - (pr_index + 1));
        	//split the dstNodeIDList by ','
    		String[] dstNodeIDs = dstNodeIDList.split(",");
        	
        	// Degree of page u
        	int degree = dstNodeIDs.length;
        	
        	for (String dstNodeID : dstNodeIDs) {
            	// Create intermediate pairs: (v1, PR), (v2, PR) ...
        		Text pr_value = new Text(String.valueOf(pr/degree));
        		output.collect(new Text(dstNodeID), pr_value);
        	}
    	}
    	
    	
    	// Create intermediate pairs: (u, |v1,v2,v3...) and (u,oldPR)
    	//used in the reducer to caculate the residual error
		// "list:" is the label denoting this is a node with its dstNodeIDList
		output.collect(new Text(srcNodeID), new Text("list:" + dstNodeIDList));
		// "pr:" is the label denoting this is a node with the old PR
		output.collect(new Text(srcNodeID), new Text("pr:" + pr));
    }
}


