CS5300proj
==========
> ***For the best format of the report and the result analysis, please see raw of [THIS](https://github.com/kidchen/CS5300proj/blob/master/README.pdf)!***

#####￼Cornell University
###### CS5300 - SP14
### Fast Convergence PageRank in Hadoop Report
##### Yun Hao (yh539), Yang Yang (yy565), Chen Zhang (cz294)

#### 0. Overall Structures
1. **Preprocess the data**: before MapReduce, we need to use our netID to create our own input edge files for each different implementation.
2. **PageRank**: this is the main part of the project. It starts a MapReduce job for each pass. These MapReduce jobs will then call the Map function and Reduce function.
3. **Map**: Map function will parse the input data and transfer the output to Reduce.
4. **Reduce**: Reduce function will calculate the new PageRank value and other information (i.e. highest rank node in each block) and produce output to Map for next pass.
5. **PRCounter**: This is a Hadoop counter. It will transfer the results from Reduce to our main part PageRank. In block implementation, we also create blockCounter in each block so that we can get the highest rank node in each block.

#### 1. Input Data Preprocess
##### 1.1 Filter Parameter:
- Reject edges are based on netID: cz294 - rejectMin: 0.99 * 0.492 (0.48708)
- rejectLimit: 0.99 * 0.492 + 0.01 (0.49708)

##### 1.2 Data Format: SimpleNode:
BlockNode:
> src_node /t pagerank /t LIST(des_node)

> src_node+src_block /t pagerank /t LIST(des_node+des_block)

* Note that the pagerank we put here is 1.0, is amplified by N(total # of nodes) so in the reducer, we calculate the new pagerank value by (1 - d) + d * incoming_PR_sum, also amplified by N(total # of nodes).

#### 2. Simple Computation of PageRank
##### 2.1 MapReduce Task
Mapper get information from master node, separate sub-divided problems and pass them to each branch node.

Reducer collect the information from each branch node and updates the PR value for the node based on the PR values of its neighbor nodes, and finally pass the updated information back to the master node.

##### 2.2 Data Format
Mapper Input/ Reducer Output Format:
<srcNodeID /t PR /t dstNodeIDLIST> Mapper Output/ Reducer Input Format:
<srcNodeID: list: dstNodeIDLIST> <srcNodeID: pr: PR>
if outdegree > 0:
Use Hadoop counter to calculate the residual error of each pass.

##### 2.3 Map
Mapper basically does the pass tasks. Load information from the last MapReduce pass. (*The first pass will load from the preprocessed file)
<srcNodeID /t PR /t dstNodeIDLIST> There are three kinds of information we need to pass:

1. <srcNodeID: list:dstNodeIDLIST>
2. <srcNodeID: pr:PR>
Use those two remember the old pagerank value and list of dstNodes.
3. <dstNodeID: PR(srcNode)/degree(srcNode)> (if not a sink)

Use this infomation to caculate the new pagerank value for each node in the reducer.

##### 2.4 Reduce
Collect information from the branch node and reduce then do the computation based on information mapper passed.
There are three kinds of information we received:

1. <srcNodeID: pr:PR> --store to oldPageRank
2. <srcNodeID: list:dstNodeIDLIST> --store to dstNodeIDList 
3. <dstNodeID: PR(srcNode)/degree(srcNode)> (if not a sink)

For each coming edge from the srcNode:
pageRankSum += prevPageRank / srcDegree
<dstNodeID: PR(srcNode)/degree(srcNode)>
newPageRank = d * pageRankSum + (1 - d) Here d is the damping factor (d=0.85).
(Not divided by N because the pagerank value of preprocess file is not divided by N, so the value here is actually amplified)

##### 2.5 Calculate the Residual
At the same time, reducer will calculate the residual error of this node and add it to counter (Hadoop counter).

> ResidualError = (newPageRank-oldPageRank)/newPageRank

We need to amplify this by 10^6 to convert to long before we add it to counter. PageRank Value gets updated every time a collection of key-value pair passes the reducer, and a new residual error will be calculated for this node. After all pairs are done, we can calculate the average residual error of this Pass:

> Avg Residual Error= Sum of Residual Error/ total # of Nodes. 

After computation, output the updated information to master node using the newPageValue it calculated and the dstNodeIDList mapper passes:

##### 2.6 Result
It demonstrates the slow convergence. We found it actually converging but the residual error is still too far from 0.001 after 5 MapReduce Passes.

#### 3. Blocked Computation of PageRank
##### 3.1 Data Format
Mapper Input/ Reducer Output Format:
< u_ndoeID+u_blockID /t u_PR /t LIST(v_nodeID+v_blockID) >

(u is the srcNode and v is the dstNode: u->v) 

Mapper Output/ Reducer Input Format:
<u_blockID: list:u_nodeID /t LIST(v_nodeID+v_blockID)> 
<u_blockID: pr:u_nodeID /t u_PR>

If degree(u) > 0:
<v_blockID: v_NodeID /t u_PR /t u_nodeID+u_blockID /t u_degree>

Else u is a sink:
<u_blockID: u_NodeID /t u_PR /t u_nodeID+u_blockID /t 0>
(Denote a sink by setting its degree to 0)

##### 3.2 Map
Load information from the last MapReduce pass: (The first pass will load from the preprocessed file)
< u_ndoeID+u_blockID /t u_PR /t LIST(v_nodeID+v_blockID) > (u is the srcNode and v is the dstNode: u->v)
There are three kinds of information we need to pass:

1. <u_blockID: list:u_nodeID /t LIST(v_nodeID+v_blockID)>
2. <u_blockID: pr:u_nodeID /t u_PR>
Use those two remember the old pagerank value and information of list of dstNodes
3. If degree(u) > 0:
<v_blockID: v_NodeID /t u_PR /t u_nodeID+u_blockID /t u_degree>
Else u is a sink:
<u_blockID: u_NodeID /t u_PR /t u_nodeID+u_blockID /t 0>
(Denote a sink by setting its degree to 0)

We use this info to calculate the new pagerank value for each node in reducer.

##### 3.3 Reduce
In reduce section, we firstly collect information from the brand node, reduce and then do the computation based on the information mapper passed. We also need to use Hadoop counters to store the sum of the residual errors, sum of the number of iterations and the pr value of the highest numbered node in each block.

Since each block can have many nodes, we need created several maps as below to keep track the related information we need:

HashMap<String, Double> oldPR : <u_nodeID, u_pr> for nodes u in block B (store old pagerank value).
HashMap<String, String> v_list_map :<u_nodeID, LIST(v_nodeID+v_blockID)> for nodes u in block B used for reducer output.
HashMap<String, ArrayList<String> > BE:<v_nodeID, arraylist(nodeID_u) > for nodes u and v in block B and u->v, BE is for the edges from nodes in block B.
HashMap<String, Integer> degreeMap:<u_nodeID, u_degree> for all nodes u in block B.
HashMap<String, ArrayList<Double>> BC:<v1_nodeID, arraylist(u1_PR/u1_degree)> for u1->v1 and v1 in block B and u1 not belong to block B. BC is for boundary condition.

We do the preparations as following:
There are three kinds of information we received

1.<u_blockID: list:u_nodeID /t LIST(v_nodeID+v_blockID)>
Find the input that has info of list_v_node_block and get u_nodeID and list of nodes v from the input, store into Map v_list_map.

2. <u_blockID: pr:u_nodeID /t u_PR>
Find the input that has info of old PR of node u and put node u and the old PR value of
node u into Map oldPR.

3.<v_blockID: v_NodeID /t u_PR /t u_nodeID+u_blockID /t u_degree>
or
<u_blockID: u_NodeID /t u_PR /t u_nodeID+u_blockID /t 0>
If u, v are from the same block: BE.put(v_nodeID,update_u_nodeID_arraylist) & degreeMap.put(u_nodeID, u_degree). If they are from different block: calculate PR(u)/degree(u), BC.put(v_nodeID, update_calc_list)--boundary value, fixed.
Algorithm:
```
for ( v in B ) { 
    NPR [v] = 0; 
} 
for ( v in B ) {
    for ( u where <u,v> in BE ) { 
        if(deg[u] > 0) {
            NPR[v] += PR[u] / deg[u];
        }
    }
    for ( u, R where <u,v,R> in BC ) { 
        NPR[v] += R;
    }
    NPR[v] = d * NPR[v] + (1 – d) ; 
}
for ( v in B ) {
    localResidualError += |NPR[v]-PR[v]| / NPR[v]; 
    PR[v] = NPR[v] ;
}
```

Repeat the iteration until the localResidualError < 0.001 * PR.size(), that means the avg residual error in this block is smaller than 0.001. Use an int type numIter to record the total number of iteration of this block.

##### 3.4 Result
Jacobi PageRank Computation takes 6 passes (38 iterations per block) to converge.

#### 4. Extra Credit
##### 4.1 Gauss-Seidel Iteration
The Gauss-Seidel Iteration method uses the most recent pagerank values wherever possible to improve convergence rate. According to the following equation:
The only difference between Gauss Iteration and Blocked Iteration is the following implementation:
double u_pr = (NPR.get(u_nodeID) > 0) ? NPR.get(u_nodeID) : PR.get(u_nodeID); This code will get the most recent pagerank values wherever possible.

￼￼￼￼The two versions take the similar mapreduce passes to converge to the same rate. But since Gauss-Seidel uses the new pagerank value as soon as possible, it needs less average number of iterations (30) per block than Jacobi, thus helping to converge faster than Jacobi.

##### 4.2 Random Block Partition
Random Hash Function:
< Hash(nodeID) = (nodeID+random)%68
The results shows that it takes 21 mapreduce passes(44 iterations per block) to converge.

#### 5. Running our Code
We have made our own pre-filtered input files, customized for the netid cz294.

**Step 0**: Upload our pre-filtered input files to s3 bucket. You might want to create a new folder such as “data”, and store all the input files in that folder.

**Step 1**: Upload custom JAR files to s3 bucket.

**Step 2**: Create a cluster called “pageRank”, store the log files to a folder such as “s3n://bucket-name/log/”.

**Step 3**: You can start a master instance and 10 core instances.

**Step 4**: Add step “Custom JAR”. For example: Name: SimplePageRank

JAR S3 location: s3n://bucket-name/SimplePageRank.jar Arguments: s3n://bucket-name/folder-name/singlenode_edge
s3n://bucket-name/folder-name

In the “Arguments” field above, the first line is the path of the input file, the
second line is the output path. Note that we have appended “/pass0”, “/pass1”... to the output path in our code. 
For the example above, the real output path will be

> s3n://bucket-name/folder-name/pass0, s3n://bucket-name/folder-name/pass1,
...

As a result, for the output argument, please end the argument with the folder-name only,
without any other special characters like ‘/’. Otherwise, it will fail to create the output folder.

**Step 5**: Create the cluster and run the program. You should see appropriate information in stdout and log files.
