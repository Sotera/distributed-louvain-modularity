package mil.darpa.xdata.louvain.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;


/**
 * Writable class to represent community information for compressing a graph
 * by its communities.
 */
public class LouvainVertexWritable implements Writable{

	long weight;
	Map<Long,Long> edges;
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		weight = in.readLong();
		int edgeSize = in.readInt();
		edges = new HashMap<Long,Long>(edgeSize);
		for (int i =0; i< edgeSize; i++){
			edges.put(in.readLong(), in.readLong());
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(weight);
		out.writeInt(edges.size());
		for (Map.Entry<Long,Long> entry : edges.entrySet()){
			out.writeLong(entry.getKey());
			out.writeLong(entry.getValue());
		}
	}
	
	
	public static LouvainVertexWritable fromTokens(String weight,String edges){
		
		LouvainVertexWritable vertex = new LouvainVertexWritable();
		vertex.weight = Long.parseLong(weight);
		Map<Long,Long> edgeMap = new HashMap<Long,Long>();
		if (edges.length() > 0){
			for (String edgeTuple: edges.split(",")){
				String[] edgeTokens = edgeTuple.split(":");
				edgeMap.put(Long.parseLong(edgeTokens[0]), Long.parseLong(edgeTokens[1]));
			}
		}
		vertex.edges = edgeMap;
		return vertex;
	}

}
