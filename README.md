Distributed Louvain Modularity
==============================
This project is a Giraph/Hadoop implementation of a distributed version of the Louvain community detection algorithm
described in "Fast unfolding of communities in large networks” - http://arxiv.org/pdf/0803.0476.pdf 

There are three main parts to the project.

1. A giraph job, that detects communities in a graph structure.
2. A map reduce job, that compresses a graph based on its community structure.
3. A python script that sets up the cluster environment and job configuration, pipes the outputs of giraph job to the map reduce job, and pipes the output of the map reduce job to the giraph job.

The map reduce and giraph job then run in a cycle, detecting communities and compressing the graph until no significant progress is being made, and then exits.

Build
-----
Prior to building you must first download appache giraph and build a version for your cluster.  
Then install the giraph-core-with-dependencies.jar into your local mvn repository.

These are instructions for building Giraph 1.0 against CDH 4.2.0.

1. Download Giraph (http://giraph.apache.org/) -> (http://www.apache.org/dyn/closer.cgi/giraph/giraph-1.0.0)

2. Extract.

3. Find the hadoop_cdh4.1.2 profile within pom.xml and copy the entire section and paste below.

4. Edit the new section changing instances of 4.1.2 to 4.2.0 within the section.

5. From the command line at the top level type 'mvn -Phadoop_cdh4.2.0 -DskipTests clean install'

6. This will install giraph-core-1.0.0.jar in your local maven repository specifically usable for CDH 4.2.0

7. You should now be able to build the LouvainModularity job using ./build.sh

Example Run
-----------
A small example is included to verify installation and the general concept in the 'example' directory.

To run, go to the example directory and type

> ./run_example.sh

Other Information
-----------------

The graph must be stored as a bi-directional weighted graph with one vertex represented below in a 
tab-delimited file stored on hdfs.  The columns required are node id, internal node weight, and the edge 
list.  The edge list should be a comma-separated list of edges where each edge is represented by the form id:weight.  

For example...

>12345	0	1:1,2:1,9:33<br>
>1	0	12345:1<br>
>2	0	12345:1<br>
>9	0	12345:33<br>

In this case vertex 12345 has weight 0 and 3 edges.  Each edge appears identically for both its vertices.


The giraph job outputs a tab-delimited hdfs file with the following fields: id, community id, internal weight, and
edge list where the edge list in the giraph output is a list of edges to communities, and not the original edge 
list from the input.

The map reduce job outputs an hdfs file that matches the required input to the giraph job and represents an community compressed version of the graph.  Each node represents an entire community. 

For custom configuration options pass them into giraph runner as -ca arguments (see louvain.py).  
The following is a list of custom arguments and their defaults.


fs.defaultFS	
  the default hdfs file system, normally this will not need to be set as it will correctly be read from the environment.

actual.Q.aggregators (default 1)
  Number of aggregators to split the actual Q aggregator into, using multiple aggregators may improve performance 

minimum.progress	(default 0)
  The minimum reduction in total nodes changed per step seen as making progress

progress.tries	(defaul 1)
	The number of times minimum.progress can be not reached before the iteration halts.
