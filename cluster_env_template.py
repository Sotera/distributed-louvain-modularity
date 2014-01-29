# is your initial data specified as an edge list?
EDGE_INPUT_FORMAT = False
LOUVAIN_EDGE_DELIMITER="\t"
LOUVAIN_REVERSE_EDGE_DUPLICATOR="true"

# are you running on PURE YARN?
PURE_YARN=True

# specify your hdfs defaultFS
HDFS_BASE='hdfs://<your hdfs location here>'

# path to the jar containing all project code
LOUVAIN_JAR = 'louvain-giraph-1.1.0-SNAPSHOT-jar-with-dependencies.jar'
LOUVAIN_JAR_PATH = 'target/'+LOUVAIN_JAR

# number of giraph slaves, in mapreduce you will need slaves+1 mappers, on yarn you will have
# slaves + 2 containers.
GIRAPH_SLAVES=1

# reduce number of slaves by this factor at each level, over provisioning workers reduces giraph performance
# so we automatically reduce the number of workers as the data set shrinks
GIRAPH_SLAVES_DECAY=0.75 

# threads per worker
COMPUTE_THREADS='1'  

# your zoo keeper list as a comma separated list.  For example 'node1:2181,node2:2181'
ZK_LIST=''
ZK_TIMEOUT='600000'

# use out of core graph?  use only for graphs to large to fit into memory
OUT_OF_CORE_GRAPH='false'

# use out of core messages?  use only when messages exceed memory
OUT_OF_CORE_MESSAGES='false'

# memory per worker for mapreduce or YARN
MAPREDUCE_HEAP='-Xmx1g'  #ignored for pure_yarn, sets mapred.child.java.opts
YARN_HEAP='1000'  #in MB, ignored if not pure_yarn