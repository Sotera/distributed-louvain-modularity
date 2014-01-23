import os
import sys
import subprocess
import time
from cluster_env import * #contains all cluster/environment specific settings for the job.


def main(ip,op):
  print ""
  print "running full louvain on "+ip
  print "saving output to: "+op
  print ""
  
  print "Removing ouput dir"
  subprocess.call(["hadoop","fs","-rm","-r",op])
  print ""
  giraph_times = []
  mapred_times = []
  error = 0
  complete = False
  i = 0
  while not complete:
    i = i + 1
    
    if error:
      print 'Exit due to non-zero return status'
      break

    if 1 == i:
      (error,elapsed) = giraph(ip,op+"/giraph_1")
      giraph_times.append(elapsed)
    else:
      (error,elapsed) = giraph(op+"/mapreduce_"+str(i-1),op+"/giraph_"+str(i))
      giraph_times.append(elapsed)
   
    complete = (0 == subprocess.call(['hadoop','fs','-ls',op+'/_COMPLETE']))

    if not complete and not error:
      (error,elapsed) = mapreduce(op+"/giraph_"+str(i),op+"/mapreduce_"+str(i))
      mapred_times.append(elapsed)

  giraph_total = reduce(lambda x,y: x+y,giraph_times)
  mapred_total = 0
  if len(mapred_times) >0:
    mapred_total = reduce(lambda x,y: x+y,mapred_times)
  total = giraph_total+mapred_total
  print "giraph times "+",".join(map(lambda x: str(x),giraph_times))
  print "total giraph time: "+str(giraph_total)
  print "mapreduce times "+",".join(map(lambda x: str(x),mapred_times))
  print "total mapreduce time "+str(mapred_total)
  print "Total time: "+str(total)



def giraph(ip,op):
  ip = HDFS_BASE+ip
  op = HDFS_BASE+op
  print "\nrunning giraph phase "+ip+" -> "+op+"\n"
  global GIRAPH_SLAVES  
  t1 = time.time()
  subprocess.call(['hadoop','fs','-rm','-r',op])
  giraph_job_args = []
  giraph_job_args.append('hadoop')
  giraph_job_args.append('jar')
  giraph_job_args.append(LOUVAIN_JAR_PATH)
  giraph_job_args.append('org.apache.giraph.GiraphRunner')
  giraph_job_args.append('-Dgiraph.numComputeThreads='+COMPUTE_THREADS)
  giraph_job_args.append('-Dgiraph.zkList='+ZK_LIST)
  giraph_job_args.append('-Dgiraph.zkSessionMsecTimeout='+ZK_TIMEOUT)
  if not PURE_YARN:
    giraph_job_args.append('-Dgiraph.useSuperstepCounters=false') 
  giraph_job_args.append('-Dgiraph.useOutOfCoreGraph='+OUT_OF_CORE_GRAPH)
  giraph_job_args.append('-Dgiraph.useOutOfCoreMessages='+OUT_OF_CORE_MESSAGES)
  if not PURE_YARN:
    giraph_job_args.append('-Dmapred.child.java.opts=-Xmx1g')
  giraph_job_args.append('mil.darpa.xdata.louvain.giraph.LouvainVertexComputation')
  if PURE_YARN:
    giraph_job_args.append('-yj')
    giraph_job_args.append(LOUVAIN_JAR)
    giraph_job_args.append('-yh')
    giraph_job_args.append(YARN_HEAP)
  giraph_job_args.append('-w')
  giraph_job_args.append(str(GIRAPH_SLAVES))
  giraph_job_args.append('-mc')
  giraph_job_args.append('mil.darpa.xdata.louvain.giraph.LouvainMasterCompute')
  giraph_job_args.append('-vif')
  giraph_job_args.append('mil.darpa.xdata.louvain.giraph.LouvainVertexInputFormat')
  giraph_job_args.append('-vof')
  giraph_job_args.append('mil.darpa.xdata.louvain.giraph.LouvainVertexOutputFormat')
  giraph_job_args.append('-vip')
  giraph_job_args.append(ip)
  giraph_job_args.append('-op')
  giraph_job_args.append(op)
  giraph_job_args.append('-ca')
  giraph_job_args.append('giraph.vertex.input.dir='+ip)
  giraph_job_args.append('-ca')
  giraph_job_args.append('mapreduce.task.timeout=10800000')
  giraph_job_args.append('-ca')
  giraph_job_args.append('actual.Q.aggregators=1')
  giraph_job_args.append('-ca')
  giraph_job_args.append('minimum.progress=2000')
  giraph_job_args.append('-ca')
  giraph_job_args.append('progress.tries=1')
  
  print 'running: ',giraph_job_args
  result = subprocess.call(giraph_job_args)
  
  # Adjust as needed to reduce slaves allocated as workload decreases.
  GIRAPH_SLAVES=max(2,int(GIRAPH_SLAVES*GIRAPH_SLAVES_DECAY))
  elapsed = time.time() - t1
  print "giraph exit status: "+str(result)+" time (sec): "+str(elapsed)
  return (result,elapsed)


def mapreduce(ip,op):
  ip = HDFS_BASE+ip
  op = HDFS_BASE+op
  print "\nrunning mapreduce phase "+ip+" -> "+op+"\n"

  print "delete output dir: "+op
  t1 = time.time()
  subprocess.call(['hadoop','fs','-rm','-r',op]) 


  result = subprocess.call(['hadoop','jar', 
    LOUVAIN_JAR_PATH,
    'mil.darpa.xdata.louvain.mapreduce.CommunityCompression',
    ip, op])


  subprocess.call(['hadoop','fs','-rm','-r',op+'/_logs'])
  elapsed = time.time() - t1
  print "mapreduce exit status: "+str(result)+" time (sec): "+str(elapsed)
  return (result,elapsed)


if __name__ == '__main__':
  if len(sys.argv) < 3 or sys.argv[1] =='-h':
    print "usage: "
    print " to run full louvain"
    print "\t"+sys.argv[0]+" <hdfs input path> <hdfs output dir> <# passes>"
    print " run a signle giraph pass"
    print "\t"+sys.argv[0]+" -giraph <hdfs input path> <hdfs output path>"
    print " run a single mapreduce pass"
    print "\t"+sys.argv[0]+" -mapreduce <hdfs input path> <hdfs output path>"
    sys.exit(1)


  if "-giraph" == sys.argv[1]:
    giraph(sys.argv[2],sys.argv[3])
  elif "-mapreduce" == sys.argv[1]:
    mapreduce(sys.argv[2],sys.argv[3])
  else:
    main(sys.argv[1],sys.argv[2])
  
