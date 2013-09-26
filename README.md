Distributed Louvain Modularity
==============================
This project is a Giraph/Hadoop implementation of a distributed version of the Louvain community detection algorithm
described in "Fast unfolding of communities in large networks” - http://arxiv.org/pdf/0803.0476.pdf 

There are three main parts to the project.

1.  A giraph job, that detects communities in a graph structure.
2.  A map reduce job, that compresses a graph based on its community structure.
3.  A python script that sets up the cluster environment and job configuration, pipes the outputs of giraph job to the map reduce job, and pipes the output of the map reduce job to the giraph job.

The map reduce and giraph job then run in a cycle, detecting communities and compressing the graph until no significant progress is being made, and then exits.

Build
-----
