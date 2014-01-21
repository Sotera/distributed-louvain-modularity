package mil.darpa.xdata.louvain.giraph;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


/**
 * Master compute class. performs a compute function before each super step. Performs 4 functions.
 *
 * 1.  prints to its standard out the number of nodes that have changed community in each pass
 * 2.  prints to its standard out the Q value of the graph when this phase is complete.
 * 3.  halts the computation when no further progress is being made
 *     (each vertex makes the same decision to halt independently on the previous step, and then aggregate their q values)
 * 4.  Determines if this should be the final phase of computation in the pipeline, if so writes a file to indicate such.
 *
 *
 * @author Eric Kimbrel - Sotera Defense
 *
 */
public class LouvainMasterCompute extends DefaultMasterCompute{

        // track the number of nodes that changed community at each iteration.
        ArrayList<Long> history = new ArrayList<Long>();

        // halt on next super step
        boolean halt = false;

        double previousQ;


        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
                this.registerAggregator(LouvainVertexComputation.CHANGE_AGG, LongSumAggregator.class);
                this.registerPersistentAggregator(LouvainVertexComputation.TOTAL_EDGE_WEIGHT_AGG, LongSumAggregator.class);
                //for (int i =0; i < LouvainVertex.getNumQAggregators(getConf()); i++){
                	this.registerPersistentAggregator(LouvainVertexComputation.ACTUAL_Q_AGG, DoubleSumAggregator.class);
                //}
                
        }



        @Override
        public void compute(){

                long superstep = getSuperstep();
                int minorstep = (int) (superstep % 3);
                int iteration = (int) (superstep / 3);

            if (superstep == 0){
                previousQ = this.getPreviousQvalue();
                System.out.println("Previous Q value: "+previousQ);
            }


        

                if (superstep == 1){
                        long m = ( (LongWritable) getAggregatedValue(LouvainVertexComputation.TOTAL_EDGE_WEIGHT_AGG)).get();
                        System.out.println("Graph Weight = "+m);
                }

                else if (minorstep ==1 && iteration > 0 && iteration % 2 == 0){
                        long totalChange = ( (LongWritable) getAggregatedValue(LouvainVertexComputation.CHANGE_AGG)).get();
                        history.add(totalChange);
                        halt = decideToHalt(history,getConf());
                        if (halt){
                        	System.out.println("superstep: "+superstep+" decided to halt.");
                        }
                        System.out.println("superstep: "+superstep+" pass: "+(iteration/2)+" totalChange: "+totalChange);

                }
                else if (halt){
                        double actualQ = getActualQ();
                        System.out.println("superstep: "+superstep+" ACTUAL Q: "+actualQ);
                        this.haltComputation();


                        writeQvalue(Double.toString(actualQ));
                        int clippedQ = (int) (actualQ *10000);
                        int clippedPreviousQ = (int) (previousQ*10000);
                        if (superstep <= 14 || clippedQ <= clippedPreviousQ){
                                markPipeLineComplete(Double.toString(actualQ));
                        }
                }
                
        }

        private double getActualQ(){
        	 double actualQ = 0.0;
        	 //for (int i =0; i < LouvainVertex.getNumQAggregators(getConf()); i++){
        		 actualQ += ( (DoubleWritable) getAggregatedValue(LouvainVertexComputation.ACTUAL_Q_AGG) ).get();
             //}
        	 return actualQ;
        }

      /**
         * Determine if progress is still being made or if the
         * computation should halt.
         * @param history
         * @return
         */
        protected static boolean decideToHalt(List<Long> history, Configuration conf){
        		int minProgress  = conf.getInt("minimum.progress", 0);
        		int tries = conf.getInt("progress.tries", 1);
        		
        	
        		// Halt if the most recent change was 0
                if (0 == history.get(history.size()-1)){
                        return true;
                }

                //Halt if the change count has increased 4 times
                long previous = history.get(0);
                int count = 0;
                for (long current : history){
                        if (current >= previous - minProgress){
                                count ++;
                        }
                        previous = current;
                }
                return (count > tries);
        }


        /**
         * Saves a file in the hdfs output dir to make that computation is complete.
         * Writes final q value to the file.
         * @param message
         */
        private void markPipeLineComplete(String message){
                String outputPath = getConf().get("mapred.output.dir");
                String dir = outputPath.substring(0, outputPath.lastIndexOf("/"));
                String filename = getConf().get("fs.defaultFS")+dir+"/_COMPLETE";
                //String filename = "hdfs://xd-namenode:8020"+dir+"/_COMPLETE";
                System.out.println("Writing "+filename);
                writeFile(filename,message);
        }



        private void writeQvalue(String message){
                String outputPath = getConf().get("mapred.output.dir");
                int lastIndexOfSlash = outputPath.lastIndexOf("/");
                String dir = outputPath.substring(0, lastIndexOfSlash);
                String stage = outputPath.substring(lastIndexOfSlash+1);
                String stagenumber = stage.substring(stage.lastIndexOf("_")+1);
                String filename = getConf().get("fs.defaultFS")+dir+"/_q_"+stagenumber;
                //String filename = "hdfs://xd-namenode:8020"+dir+"/_q_"+stagenumber;
                writeFile(filename,message);

        }


       private double getPreviousQvalue(){
                String outputPath = getConf().get("mapred.output.dir");
                int lastIndexOfSlash = outputPath.lastIndexOf("/");
                String dir = outputPath.substring(0, lastIndexOfSlash);
                String stage = outputPath.substring(lastIndexOfSlash+1);
                String stagenumber = stage.substring(stage.lastIndexOf("_")+1);
                int previousStageNumber = Integer.parseInt(stagenumber) - 1;
                if (previousStageNumber < 1){
                        return 0.0;
                }
                else{
                        String filename = getConf().get("fs.defaultFS")+dir+"/_q_"+previousStageNumber;
                        //String filename = "hdfs://xd-namenode:8020"+dir+"/_q_"+previousStageNumber;
                        String result = this.readFile(filename).trim();
                        return Double.parseDouble(result);
                }
        }


        private void writeFile(String path,String message){
                Path pt = new Path(path);
                try {
                        FileSystem fs = FileSystem.get(new Configuration());
                        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
                        br.write(message);
                        br.close();
                } catch (IOException e) {
                        e.printStackTrace();
                        throw new IllegalStateException("Could not write to file: "+path);
                }
        }


        private String readFile(String path){
                StringBuilder builder = new StringBuilder();
                try{
                        Path pt = new Path(path);
                        FileSystem fs = FileSystem.get(new Configuration());
                        BufferedReader br= new BufferedReader(new InputStreamReader(fs.open(pt)));
                        String line;
                        line = br.readLine();
                        while( line != null){
                                builder.append(line);
                                line = br.readLine();
                        }
                } catch(Exception e){
                        throw new IllegalStateException(" Could not read file: "+path);
                }
                return builder.toString();
        }



}



