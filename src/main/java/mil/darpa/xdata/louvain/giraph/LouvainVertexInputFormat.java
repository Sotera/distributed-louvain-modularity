package mil.darpa.xdata.louvain.giraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;


import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;




/**
 * Reads in a graph from text file in hdfs.  Required formatin is a tab delimited file with 3 columns
 * id	internal weight		edge list
 * 
 * the edge list is a comma seperated list of edges of the form    id:weight
 * 
 * The graph must be bi-directional   i.e.  if vertex 1 has edge 2:9, the vertex 2 must have id 1:9
 * This condition is not verified as the input is read, but results of the algorithim will not be correct,
 * and the run may fail with expceptions.  
 * 
 *
 */
public class LouvainVertexInputFormat extends TextVertexInputFormat<LongWritable,LouvainNodeState,LongWritable>{

	@Override
	public TextVertexReader createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new LouvainVertexReader();
	}
	
	protected class LouvainVertexReader extends TextVertexReader{

		@Override
		public Vertex<LongWritable, LouvainNodeState, LongWritable, Writable> getCurrentVertex()
				throws IOException, InterruptedException {

			String line = getRecordReader().getCurrentValue().toString();
			String[] tokens = line.trim().split("\t");
			if (tokens.length < 2){
				throw new IllegalArgumentException("Invalid line: ("+line+")");
			}
			
			int edgeListIndex = 2;
			int internalWeightIndex = 1;
			if (tokens.length ==2){
				internalWeightIndex = -1;
				edgeListIndex = 1;
			}
			
			LouvainNodeState state = new LouvainNodeState();
			LongWritable id = new LongWritable(Long.parseLong(tokens[0]));
			state.setCommunity(id.get());
			state.setInternalWeight( (internalWeightIndex > 0) ? Long.parseLong(tokens[internalWeightIndex]) : 0L  );
			
			long sigma_tot = 0;
			Map<LongWritable,LongWritable> edgeMap = new HashMap<LongWritable,LongWritable>();
			ArrayList<Edge<LongWritable,LongWritable>> edgesList = new ArrayList<Edge<LongWritable,LongWritable>>();
            String[] edges = (edgeListIndex < tokens.length) ? tokens[edgeListIndex].split(",") : new String[0];
			for (int i = 0; i < edges.length ; i++){
				if (edges[i].indexOf(':') != -1){
					String[] edgeTokens = edges[i].split(":");
					if (edgeTokens.length != 2){
						throw new IllegalArgumentException("invalid edge ("+edgeTokens[i]+") in line ("+line+")");
					}
					long weight = Long.parseLong(edgeTokens[1]);
					sigma_tot += weight;
					Long edgeKey = Long.parseLong(edgeTokens[0]);
					edgeMap.put(new LongWritable(edgeKey), new LongWritable(weight));
                                        //edgesList.add(EdgeFactory.create(new LongWritable(edgeKey),new LongWritable(weight)));
				}
				else{
					Long edgeKey = Long.parseLong(edges[i]);
					Long weight = 1L;
					sigma_tot += weight;
					edgeMap.put(new LongWritable(edgeKey), new LongWritable(weight));
                                        //edgesList.add(EdgeFactory.create(new LongWritable(edgeKey),new LongWritable(weight))); 
				}
				
			}
			state.setCommunitySigmaTotal(sigma_tot+state.getInternalWeight());
			state.setNodeWeight(sigma_tot);
			

                        for (Map.Entry<LongWritable,LongWritable> entry : edgeMap.entrySet()){
                          edgesList.add(EdgeFactory.create(entry.getKey(),entry.getValue()));
                        }

			Vertex<LongWritable, LouvainNodeState, LongWritable, Writable>  vertex = this.getConf().createVertex();
			vertex.initialize(id, state,edgesList);
			
			return vertex;
		
		}
			
		

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}
		
	}

}
