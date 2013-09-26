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
