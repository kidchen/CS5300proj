import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class BlockedPageRankCalculateMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{    
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// find u_nodeID and u_blockID from the first element of input, seperate by "+"
    	int u_nodeID_blockID_index = value.find("\t");
    	String u_nodeID_blockID = Text.decode(value.getBytes(), 0, u_nodeID_blockID_index);
    	int block_index = u_nodeID_blockID.indexOf("+");
    	
    	String u_nodeID = u_nodeID_blockID.substring(0, block_index);
    	String u_blockID = u_nodeID_blockID.substring(block_index + 1, u_nodeID_blockID.length());
    	// find the PageRank value of u from the second element of input
    	int u_pr_index = value.find("\t", u_nodeID_blockID_index + 1);
    	Double u_pr = Double.parseDouble(Text.decode(value.getBytes(), u_nodeID_blockID_index + 1, u_pr_index - (u_nodeID_blockID_index+1)));

    	String listOfv_nodeID_blockID = "";
    	// find the LIST(nodeID_v_+blockID_v) from the third element if u is not a sink
    	//split the LIST by "," and store in array in the format of v_nodeID+v_blockID
    	//get v_nodeID and v_blockID from the array
    	//output intermediate pairs: (v_blockID; v_nodeID u_pr u_nodeID+u_blockID degree)
    	if (value.getLength() > (u_pr_index + 1)) {
    		listOfv_nodeID_blockID = Text.decode(value.getBytes(), u_pr_index + 1, value.getLength() - (u_pr_index + 1));
        	String[] arrayOfv = listOfv_nodeID_blockID.split(",");
        	
        	// Degree of page u
        	int u_degree = arrayOfv.length;
        	for (String v_nodeID_blockID : arrayOfv) {
        		//get v_nodeID and v_blockID from array of v seperated by "+"
            	int block_v_index = v_nodeID_blockID.indexOf("+");
            	String v_nodeID = v_nodeID_blockID.substring(0, block_v_index);
            	String v_blockID = v_nodeID_blockID.substring(block_v_index + 1, v_nodeID_blockID.length());
            	//output intermediate pairs: (v_blockID; v_nodeID u_pr u_nodeID+u_blockID degree)
        		Text value_for_page_v = new Text(v_nodeID + "\t" + u_pr + "\t" + u_nodeID_blockID + "\t" + u_degree);
        		output.collect(new Text(v_blockID), value_for_page_v);
        		
        	}
    	}
    	//if node u is a sink then set the degree to 0
    	//output intermediate pairs: (u_blockID; u_nodeID u_pr u_nodeID+u_blockID 0)
    	else {
    		output.collect(new Text(u_blockID), new Text(u_nodeID + "\t" + u_pr + "\t" + u_nodeID_blockID + "\t" + 0));
    		
    	}
    	
    	// output intermediate pairs: (u_blockID;u_nodeID LIST(v_nodeID+v_blockID))
    	// "|" is the label denoting this is a node with its dstNodeIDList
		output.collect(new Text(u_blockID), new Text("links" + u_nodeID + "\t" + listOfv_nodeID_blockID));
		// output intermediate pairs: (u_blockID;u_nodeID u_u_pr)
		// "!" is the label denoting this is a node with the old u_pr
		output.collect(new Text(u_blockID), new Text("PageRanks" + u_nodeID + "\t" + u_pr));
    }
}

