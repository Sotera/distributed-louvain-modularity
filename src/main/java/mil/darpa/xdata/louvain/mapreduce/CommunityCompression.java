package mil.darpa.xdata.louvain.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;




/**
 * Map reduce job to compresse a graph in such a way that each community is represetned by a single node.
 * 
 * input format:  see LouvainVertexOutputFormat
 * 
 * output format  see LouvainVertexInputFormat
 * 
 * *** the input to this job is output of the BSP computation, the ouput of this job is the input to the next stage of BSP.
 * 
 * @author Eric Kimbrel - Sotera Defense
 *
 */
public class CommunityCompression {

	
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, LouvainVertexWritable> {
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, LouvainVertexWritable> output, Reporter reporter) throws IOException {
			
			String[] tokens = value.toString().trim().split("\t");
			if (3 > tokens.length){
				throw new IllegalArgumentException("Expected 4 cols: got "+tokens.length+"  from line: "+tokens.toString());
			}

			LongWritable outKey = new LongWritable(Long.parseLong(tokens[1])); // group by community
			String edgeListStr = (tokens.length == 3) ? "" : tokens[3];
			LouvainVertexWritable outValue = LouvainVertexWritable.fromTokens(tokens[2], edgeListStr);
			output.collect(outKey, outValue);
		}
	}
	
	

	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, LouvainVertexWritable, Text, Text> {
		
		public void reduce(LongWritable key, Iterator<LouvainVertexWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			long communityId = key.get();
			long weight = 0;
			HashMap<Long,Long> edgeMap = new HashMap<Long,Long>();
			while (values.hasNext()){
				LouvainVertexWritable vertex = values.next();
				weight += vertex.weight;
				for (Entry<Long, Long> entry : vertex.edges.entrySet()){
					long entrykey = entry.getKey();
					
					if (entrykey == communityId){
						weight += entry.getValue();
					}
					else if (edgeMap.containsKey(entrykey)){
						long w = edgeMap.get(entrykey) + entry.getValue();
						edgeMap.put(entrykey, w);
					}
					else{
						edgeMap.put(entry.getKey(), entry.getValue());
					}
				}
			}
			
			StringBuilder b = new StringBuilder();
			b.append(weight).append("\t");
			for (Entry<Long, Long> entry : edgeMap.entrySet()){
				b.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
			}
			b.setLength(b.length() - 1);
			
			output.collect(new Text(key.toString()), new Text(b.toString()));
			
		}
	}
	
	
	
	public static void main(String [] args) throws Exception {
		JobConf conf = new JobConf(CommunityCompression.class);
		conf.setJobName("Louvain graph compression");
		
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LouvainVertexWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
	
}
