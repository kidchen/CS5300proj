import java.io.IOException;
import java.util.Iterator;
import java.lang.Math;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BlockedPageRankCalculateReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private static final double damping = 0.85F;
    
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		//key - the key.
		//values - the list of values to reduce.
		//output - to collect keys and combined values.
		//reporter - facility to report progress.
		
		// HashMap of <u_nodeID, u_pr> for nodes u in block B 
        // used in residual error calculation
		HashMap<String, Double> oldPR = new HashMap<String, Double> ();
		// HashMap of <u_nodeID, LIST(v_nodeID+v_blockID)> for nodes u in block B
		// used for output
    	HashMap<String, String> v_list_map = new HashMap<String, String> ();
    	
    	// HashMap of <v_nodeID, arraylist(nodeID_u) >
    	// for nodes u and v in block B and u->v
    	// BE is for the edges from nodes in block B
    	HashMap<String, ArrayList<String> > BE = new HashMap<String, ArrayList<String> > ();
    	
    	// HashMap of <u_nodeID, u_degree>
        // for all nodes u in block B
    	HashMap<String, Integer> degreeMap = new HashMap<String, Integer> ();
    	
    	// HashMap of <v1_nodeID, arraylist(u1_PR/u1_degree)>
    	// for u1->v1 and v1 in block B and u1 not belong to block B
    	// BC is for boundary condition
    	HashMap<String, ArrayList<Double>> BC = new HashMap<String, ArrayList<Double>> ();
    	
    	String value;
        while (values.hasNext()) {
        	value = values.next().toString();
        	// find the input that has info of listOfv_nodeID_blockID
        	//get u_nodeID and list of nodes v from the input, store into Map v_list_map
        	if (value.startsWith("links")) {
        		int start_list_v_index = value.indexOf("\t");
        		String u_nodeID = value.substring(5, start_list_v_index);
        		String list_v = value.substring(start_list_v_index + 1);
        		v_list_map.put(u_nodeID, list_v);
        		continue;
        	}
        	// find the input that has info of old PR of node u
        	//put node u and the old PR value of node u into Map oldPR
        	if (value.startsWith("PageRanks")) {
        		int prIndex = value.indexOf("\t");
        		String u_nodeID = value.substring(9, prIndex);
        		Double oldPRValue = Double.parseDouble(value.substring(prIndex + 1));
        		oldPR.put(u_nodeID, oldPRValue);
        		continue;
        	}
        	
        	// find the input to calculate the new page rank(two kinds: sink and no sink)
        	int v_nodeID_index = value.indexOf("\t");
        	//find the v_nodeID form the input, the first element
        	String v_nodeID = value.substring(0, v_nodeID_index);
        	int u_pr_index = value.indexOf("\t", v_nodeID_index + 1);
        	// get the pr value of node u from the second element in input
        	double u_pr = Double.parseDouble(value.substring(v_nodeID_index + 1, u_pr_index));
        	//get u_nodeID and u_nodeBlockID from the third element in input
        	int u_nodeID_blockID_index = value.indexOf("\t", u_pr_index + 1);
        	String u_nodeID_blockID = value.substring(u_pr_index + 1, u_nodeID_blockID_index);
        	int u_nodeID_Index = u_nodeID_blockID.indexOf("+");
        	String u_nodeID = u_nodeID_blockID.substring(0, u_nodeID_Index);
        	String u_blockID = u_nodeID_blockID.substring(u_nodeID_Index + 1);
        	//get the degree from u from the last element of input
        	int u_degree = Integer.parseInt(value.substring(u_nodeID_blockID_index + 1));
        	
        	// If u, v from the same block, add it into BE and record the degree of u
        	if (u_blockID.equals(key.toString())) {
        		if (BE.containsKey(v_nodeID)) {
        			ArrayList<String> list = BE.get(v_nodeID);
        			list.add(u_nodeID);
        			BE.put(v_nodeID, list);
        		}
        		else {
            		ArrayList<String> list = new ArrayList<String>();
            		list.add(u_nodeID);
            		BE.put(v_nodeID, list);
            	}
        		degreeMap.put(u_nodeID, u_degree);
        	}
        	// If they are from different block, calculate PR(u)/PR(u) and record the boundary value 
        	else {
        		if (BC.containsKey(v_nodeID)) {
        			ArrayList<Double> list = BC.get(v_nodeID);
        			list.add(u_pr / u_degree);
        			BC.put(v_nodeID, list);
        		}
        		else {
            		ArrayList<Double> list = new ArrayList<Double>();
            		list.add(u_pr / u_degree);
            		BC.put(v_nodeID, list);
            	}
        	}
        }
        
        //Constructs a new HashMap with the same mappings as Map oldPR
        HashMap<String, Double> PR = new HashMap<String, Double> (oldPR);
        //initialzie the localResidual with PR.size() to make sure that the first iteration will go:localResidual > 0.001 * PR.size()
        //when localResidual of each iteration is smaller than 0.001*(number of nodes in block B), the iteration will stop
        //notice that the pr value in our initial propressed file is not divided by the total num of nodes
        double localResidual = PR.size();
        // record the number of iterations it takes for block B
        int numIter = 0;
        while (localResidual > 0.001 * PR.size()) {
        	++ numIter;
            localResidual = 0.0;
            
            //initialize the Map NPR with pr value of 0.0 for each iteration
        	HashMap<String, Double> NPR = new HashMap<String, Double> (PR);
        	for(String tempPage: NPR.keySet()){
        		NPR.put(tempPage, 0.0);
        	}
        	
        	// first iterate the BE Map to calculate the income from the same block for each node v in Block B
        	for(String v_nodeID: BE.keySet()) {
        		ArrayList<String> arraylst_u_nodeID = BE.get(v_nodeID);
        		double pr_value = 0;
        		for (String u_nodeID : arraylst_u_nodeID) {
        			double u_pr = (NPR.get(u_nodeID) > 0) ? NPR.get(u_nodeID) : PR.get(u_nodeID);
        			int u_degree = degreeMap.get(u_nodeID);
        			// if u is not a sink, than add PR(u)/Degree(u) to v's pr value
        			// pr_value += (dgr == 0) ? pr : pr/dgr;
        			pr_value += (u_degree == 0) ? 0 : u_pr/u_degree;
        		}
        		//update pr value of node v in new pr value Map
       			NPR.put(v_nodeID, pr_value);
        	}
        	
        	//iterate BC to get the income from nodes outside the block B for nodes v in block B
        	//add the income and update the PR value in New PR value Map
        	for(String page_v: BC.keySet()) {
        		ArrayList<Double> edges_out_block = BC.get(page_v);
        		double pr_value = 0;
        		pr_value = NPR.get(page_v);
        		for (Double R : edges_out_block) {
        			pr_value += R;
        		}
        		NPR.put(page_v, pr_value);
        	}
        	
        	// after calculting all the pr value bring by income
        	//iterate NPR to update the new pr value with damping factor
        	for(String page_v: NPR.keySet()) {
        		double pr = NPR.get(page_v);
        		pr = damping * pr + (1 - damping);
        		NPR.put(page_v, pr);
        	}
        	
        	// Update PR using NPR, put the new pagerank value to PR Map
        	//also calculate localResidualError
        	for(String page_v: NPR.keySet()){
        		double pr = NPR.get(page_v);
        		localResidual += Math.abs(PR.get(page_v) - pr) / pr;
        		PR.put(page_v, pr);
        	}
        }
        
        //calculte the local residual value after the last iteration
       	double residualValue = 0.0;
    	for(String page_v: PR.keySet()) {
    		double pr = PR.get(page_v);
    		double old_pr = oldPR.get(page_v);
           	residualValue += Math.abs(pr - old_pr) / pr;
    	}
    	
    	//after amplify local residual error by 10^6 and convert it to long type
    	//add it to counter GLOBAL_RESIDUAL_COUNTER
    	//each block complete their iteration and add local residual error to counter GLOBAL_RESIDUAL_COUNTER
    	//each block complete their iteration and add local iteration number to counter LOCAL_ITERATION_NUM_COUNTER
    	//each block complete their iteration and add counter BLOCK_COUNTER by 1
    	//we can use LOCAL_ITERATION_NUM_COUNTER and BLOCK_COUNTER to calculate the avg iteration number of each block take in one MapReduce Pass
        long amplifiedValue = (long)Math.round(residualValue * 10e6);
        reporter.incrCounter(MyCounter.GLOBAL_RESIDUAL_COUNTER, amplifiedValue);
        reporter.incrCounter(MyCounter.LOCAL_ITERATION_NUM_COUNTER, numIter);
        reporter.incrCounter(MyCounter.BLOCK_COUNTER, 1);
        
        // Generate output in format: <u_nodeID+u_blockID   u_PR    LIST(v_nodeID+v_blockID)
    	for(String u_nodeID: PR.keySet()) {
    		String u_nodeID_blockID = u_nodeID + "+" + key.toString();
    		double u_pr = PR.get(u_nodeID);
    		String links = v_list_map.get(u_nodeID);
    		output.collect(new Text(u_nodeID_blockID), new Text(u_pr + "\t" + links));
    	}
    	
    	// Get the highest numbered node id for each block
    	String highestNodeID = "";
    	for(String node_id: PR.keySet()){
    		if (node_id.compareTo(highestNodeID)>0) {
    			highestNodeID = node_id;
    		}    		
    	}
    	
    	//get the pr of this node and amplified by 10^6 and convert to long type, add to the corresponding counter
    	double highestNodePR =PR.get(highestNodeID);
    	long amplifiedhighestNodePR = (long)Math.round( highestNodePR * 10e6);
    	int currentBlock = Integer.parseInt(key.toString());
    	reporter.incrCounter(HighestRankCounter.values()[currentBlock], amplifiedhighestNodePR);
    }
}







