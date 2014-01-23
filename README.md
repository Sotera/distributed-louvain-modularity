Distributed Louvain Modularity
==============================
This project is a Giraph/Hadoop implementation of a distributed version of the Louvain community detection algorithm
described in "Fast unfolding of communities in large networks��� - http://arxiv.org/pdf/0803.0476.pdf 

There are three main parts to the project.

1. A giraph job, that detects communities in a graph structure.
2. A map reduce job, that compresses a graph based on its community structure.
3. A python script that sets up the cluster environment and job configuration, pipes the outputs of giraph job to the map reduce job, and pipes the output of the map reduce job to the giraph job.

The map reduce and giraph job then run in a cycle, detecting communities and compressing the graph until no significant progress is being made, and then exits.

Running on PURE YARN (or not)
-----
The steps below assume you are running a YARN cluster and wish to run giraph in PURE_YARN mode and not on top of mapreduce.
If you want to run giraph on mapreduce instead:

1.  omit the hadoop_yarn flag from the giraph build

2.  prior to running the louvain.py script ensure the PURE_YARN flag is set to False in your cluster_env.py file


Build
-----
This build depends on giraph-1.1.0(-SNAPSHOT), if you want a more stable version using giraph 1.0 please see our other
branches in git-hub.  These build instructions assume you want to run giraph on YARN, if you want to use giraph on mapreduce instead 
omit the hadoop_yarn flag from your build and set the PURE_YARN flag to False in the pythong run scripts prior to running on a cluster.

Prior to building you must first download appache giraph and build a version for your cluster.  
Then install the giraph-core-with-dependencies.jar into your local mvn repository.

These are instructions for building Giraph 1.1.0-SNAPSHOT against hadoop 2.2.0-cdh5.0.0-beta-1 

1. Get the code for giraph 1.1.0(-SNAPSHOT) from https://github.com/apache/giraph/.  Get the latest code and not the 1.0 release

2. Check on the status of GIRAPH-819  https://issues.apache.org/jira/browse/GIRAPH-819  if this fix has not been included in the code base
   you'll need to apply the patch yourself.


3. From the command line at the top level type 'mvn -Dhadoop.version=2.2.0-cdh5.0.0-beta-1 -DskipTests -Phadoop_yarn clean install'.  
   This will install giraph-core-1.0.0.jar in your local maven repository specifically usable for CDH 5.0.0 on YARN.  You only need
   giraph-core, if Giraph Distribution fails to build its okay.

4. You should now be able to build the LouvainModularity job using ./build.sh or by running 'mvn clean install  assembly:single'
   After building verify target/louvain-giraph-1.1.0-SNAPSHOT-jar-with-dependencies.jar exists.  

Example Run
-----------

A small example is included to verify installation and the general concept in the 'example' directory.

To run:

1. In the project root cp cluster_env_template.py to cluster_env.py

2. Edit the settings in clsuter_env.py for your environemnt

3. Create a directory to use in hdfs at /tmp/louvain-giraph-example,  give the yarn user access to this directory

4. go to the example directory and type

> sudo -u yarn ./run_example.sh

Other Information
-----------------

The graph must be stored as a bi-directional weighted graph with one vertex represented below in a 
tab-delimited file stored on hdfs.  The columns required are node id, internal node weight, and the edge 
list.  The edge list should be a comma-separated list of edges where each edge is represented by the form id:weight.  

For example...

> 12345	0	1:1,2:1,9:33<br>
1	0	12345:1<br>
2	0	12345:1<br>
9	0	12345:33<br>

In this case vertex 12345 has weight 0 and 3 edges.  Each edge appears identically for both its vertices.


The giraph job outputs a tab-delimited hdfs file with the following fields: id, community id, internal weight, and
edge list where the edge list in the giraph output is a list of edges to communities, and not the original edge 
list from the input.

The map reduce job outputs an hdfs file that matches the required input to the giraph job and represents an community compressed version of the graph.  Each node represents an entire community. 

For custom configuration options pass them into giraph runner as -ca arguments (see louvain.py).  
The following is a list of custom arguments and their defaults.


> fs.defaultFS	
  
  the default hdfs file system, normally this will not need to be set as it will correctly be read from the environment.

> actual.Q.aggregators (default 1)
  
  Number of aggregators to split the actual Q aggregator into, using multiple aggregators may improve performance 

> minimum.progress	(default 0)
  
  The minimum reduction in total nodes changed per step seen as making progress

> progress.tries	(default 1)
	
  The number of times minimum.progress can be not reached before the iteration halts.
