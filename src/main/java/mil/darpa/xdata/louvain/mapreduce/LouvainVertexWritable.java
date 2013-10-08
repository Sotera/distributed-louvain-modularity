package mil.darpa.xdata.louvain.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Writable class to represent community information for compressing a graph
 * by its communities.
 */
public class LouvainVertexWritable implements Writable {

	long weight;
	Map<String,Long> edges;
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		weight = in.readLong();
		int edgeSize = in.readInt();
		edges = new HashMap<String,Long>(edgeSize);
		for (int i =0; i< edgeSize; i++){
			edges.put(WritableUtils.readString(in), in.readLong());
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(weight);
		out.writeInt(edges.size());
		for (Map.Entry<String,Long> entry : edges.entrySet()){
			WritableUtils.writeString(out, entry.getKey());
			out.writeLong(entry.getValue());
		}
	}
	
	
	public static LouvainVertexWritable fromTokens(String weight,String edges){
		
		LouvainVertexWritable vertex = new LouvainVertexWritable();
		vertex.weight = Long.parseLong(weight);
		Map<String,Long> edgeMap = new HashMap<String,Long>();
		if (edges.length() > 0){
			for (String edgeTuple: edges.split(",")){
				String[] edgeTokens = edgeTuple.split(":");
				edgeMap.put(edgeTokens[0], Long.parseLong(edgeTokens[1]));
			}
		}
		vertex.edges = edgeMap;
		return vertex;
	}

}
