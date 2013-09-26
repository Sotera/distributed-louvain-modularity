import os
import sys
import subprocess
import time


GIRAPH_SLAVES=1
ZK_LIST='localhost:2181'

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
  print "\nrunning giraph phase "+ip+" -> "+op+"\n"
  global GIRAPH_SLAVES  
  t1 = time.time()
  subprocess.call(['hadoop','fs','-rm','-r',op])
  result = subprocess.call(['hadoop','jar',
    'target/louvain-giraph-0.1-SNAPSHOT-jar-with-dependencies.jar',
    'org.apache.giraph.GiraphRunner',
    '-Dgiraph.zkList='+ZK_LIST,
    '-Dgiraph.useSuperstepCounters=false',
    '-Dmapred.child.java.opts=-Xmx1g',
    'mil.darpa.xdata.louvain.giraph.LouvainVertex',
    '-w',str(GIRAPH_SLAVES),
    '-mc','mil.darpa.xdata.louvain.giraph.LouvainMasterCompute',
    '-vif','mil.darpa.xdata.louvain.giraph.LouvainVertexInputFormat',
    '-of', 'mil.darpa.xdata.louvain.giraph.LouvainVertexOutputFormat',
    '-vip',ip,
    '-op',op,
    '-ca','giraph.vertex.input.dir='+ip,
    '-ca',' mapreduce.task.timeout=10800000',
    '-ca','actual.Q.aggregators=1',
    '-ca','minimum.progress=2000',
    '-ca','progress.tries=1'
  ])
  
  # Adjust as needed to reduce slaves allocated as workload decreases.
  GIRAPH_SLAVES=max(2,int(GIRAPH_SLAVES*.75))
  elapsed = time.time() - t1
  print "giraph exit status: "+str(result)+" time (sec): "+str(elapsed)
  return (result,elapsed)


def mapreduce(ip,op):
  print "\nrunning mapreduce phase "+ip+" -> "+op+"\n"

  print "delete output dir: "+op
  t1 = time.time()
  subprocess.call(['hadoop','fs','-rm','-r',op]) 


  result = subprocess.call(['hadoop','jar', 
    'target/louvain-giraph-0.1-SNAPSHOT-jar-with-dependencies.jar',
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
  
