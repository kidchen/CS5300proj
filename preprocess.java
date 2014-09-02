import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;
import java.util.Scanner;


public class preprocess {
	
	public static void main(String []args) throws IOException{
		SingleNodePreprocess("edges.txt","singlenode_edge");
		BlockNodePreprocess("edges.txt","nodes.txt","block_edge");
		generateRandomBlocks("random_node");
		BlockNodePreprocess("edges.txt","random_node","random_block_edge");
	}
	 /*
	  * input: edge.txt
	  * output: singlenode_edge
	  */
	 public static void SingleNodePreprocess(String input,String output)throws IOException{
		 Scanner s = new Scanner(new File(input));
		 // srcNodeID is the key and dstNodeIdList is the value
		 Hashtable<Integer, ArrayList<Integer>> graph = new Hashtable<Integer, ArrayList<Integer>>();
		 // record the prevNodeID that has iterated.
		 int max = 0;
		 while(s.hasNext()){
			 String line = s.nextLine();
			 String [] seg = line.split(" ");
			 int i = 0;
			 Integer from = -1;
			 // get the first element in each line, which represent the srcNodeID
			 for(;;i++){
				 if(seg[i].length()>0){
					 from = Integer.parseInt(seg[i]);
					 i++;
					 break;
				 }
			 }
			 // update the latest srcNodeID
			 max = from;
			 Integer to = -1;
			 // get the second element in each line, which represent the dstNodeID
			 for(;;i++){
				 if(seg[i].length()>0){
					 to = Integer.parseInt(seg[i]);
					 i++;
					 break;
				 }
			 }
			 Double d = 0.0;
			 // get the third element in each line, this is used to filter the data
			 for(;;i++){
				 if(seg[i].length()>0){
					 d = Double.parseDouble(seg[i]);
					 break;
				 }
			 }
			 // netID is cz294
			 // rejectMin = 0.99*0.492  rejectLimit = 0.99*0.492+0.01
			 if(d>=0.99*0.492&&d<0.99*0.492+0.01){
				 continue;
			 }
			 ArrayList<Integer> conn = new ArrayList<Integer> ();
			 // if the hash table contain record which key is this srcNode, add the dstNode to dstNodeList and update the record
			 if(graph.containsKey(from)){
				 conn = graph.get(from);
			 }
			 conn.add(to);
			 graph.put(from, conn);
		 }
		 s.close();
		 // done with scanning the input, get the number of selected edges
		 // using the info in hash table to write the preprocessed file for simple PR
		 BufferedWriter out = new BufferedWriter(new FileWriter(output));
		 for(int i = 0;i<=max;i++){
			 ArrayList<Integer> temp = graph.get(i);
			 String sout = "";
			 if(temp!=null){
				 for(Integer t:temp){
					 sout = sout+t+",";
				 }
				 //eliminate the last ',' in the outgoingNodeIDList
				 sout = sout.substring(0,sout.length()-1);
			 }
			 out.write(i+"\t"+1.0+"\t"+sout+"\n");
		 }
		 out.close();
	 }
	 
	 /*
	  * input: edge.txt, nodes.txt
	  * output: block_edge
	  */
	 public static void BlockNodePreprocess(String edge, String node, String output)throws IOException{
		 Scanner s = new Scanner(new File(node));
		 // store block information in hash table blockinfo, key is the nodeID and value is the blockID this node belongs to
		 Hashtable<Integer, Integer> table = new Hashtable<Integer, Integer> ();
		 // store edge information in hash table edge info, key is the srcNodeID and value is the dstNodeIDList
		 Hashtable<Integer, ArrayList<Integer>> edgeinfo = new Hashtable<Integer, ArrayList<Integer>> ();
		 while(s.hasNext()){
			 String line = s.nextLine();
			 String [] data = line.split(" ");
			 // record the latest NodeID
			 Integer n = -1;
			 int i = 0;
			 // get the nodeID of each line in node.txt
			 for(;;i++){
				 if(data[i].length()>0){
					 n = Integer.parseInt(data[i]);
					 i++;
					 break;
				 }
			 }
			 Integer b = -1;
			 // get the blockID of each line in node.txt
			 for(;;i++){
				 if(data[i].length()>0){
					 b = Integer.parseInt(data[i]);
					 break;
				 }
			 }
			 // put node id and block id into the hash table blockinfo
			 table.put(n, b);
		 }
		 s.close();
		 s= new Scanner(new File(edge));
		 while(s.hasNext()){
			 String line = s.nextLine();
			 String [] data = line.split(" ");
			 int i = 0;
			 Integer from = -1;
			 Integer to = -1;
			 Double d = 0.0;
			 // get the first element in each line, which represent the srcNodeID.
			 for(;;i++){
				 if(data[i].length()>0){
					 from = Integer.parseInt(data[i]);
					 i++;
					 break;
				 }
			 }
			 // get the second element in each line, which represent the dstNodeID
			 for(;;i++){
				 if(data[i].length()>0){
					 to = Integer.parseInt(data[i]);
					 i++;
					 break;
				 }
			 }
			 // get the third element in each line, which is used in filter the data
			 for(;;i++){
				 if(data[i].length()>0){
					 d = Double.parseDouble(data[i]);
					 break;
				 }
			 }
			 // netID is cz294
			 // rejectMin = 0.99*0.492  rejectLimit = 0.99*0.492+0.01
			 if(d>=0.99*0.492&&d<0.99*0.492+0.01){
				 continue;
			 }
			 ArrayList<Integer> temp = new ArrayList<Integer>();
			 // if the hash table contain record which key is this srcNode,add the dstNode to dstNodeList and update the record
			 if(edgeinfo.containsKey(from)){
				 temp = edgeinfo.get(from);
			 }
			 temp.add(to);
			 edgeinfo.put(from, temp);
		 }
		 s.close();
		 BufferedWriter out = new BufferedWriter(new FileWriter(output));
		 // get each record by key in hash table blockinfo
		 for(int i = 0;i<table.size();i++){
			 // get the outgoingNodeIDList of this nodeId from hash table edgeinfo
			 ArrayList<Integer> temp = edgeinfo.get(i);
			 int block = table.get(i);
			 String sout = i+"+"+block+"\t"+1.0+"\t";
			 if(temp!=null){
				 for(Integer t:temp){
					 int b = table.get(t);
					 sout = sout+t+"+"+b+",";
				 }
				 // eliminate the last ',' in each line
				 sout = sout.substring(0,sout.length()-1);
			 }
			 out.write(sout+"\n");
		 }
		 out.close();
	 }
	 
	 /*
	  * output: random_node
	  */
	 // hash function: blockID = (nodeID + randomValue) % 68
	 public static void generateRandomBlocks(String output)throws IOException{
		 BufferedWriter out = new BufferedWriter(new FileWriter(output));
		 for(int i = 0;i<=685229;i++){
		 	 Random r = new Random();
			 int b = Math.abs(r.nextInt()+i)%68;
			 out.write(i+"   "+b+"\n");
		 }
		 out.close();
	 }
}
