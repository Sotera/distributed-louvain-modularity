package mil.darpa.xdata.louvain.giraph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


/**
 * Outputs the graph as text in hdfs:
 * 
 * Format is a tab seperated file with
 *  id	community id	internal weight	community edge list
 *  
 *  the edge list is a comma seperated list of edges of the form    id:weight
 * 
 */
public class LouvainVertexOutputFormat extends TextVertexOutputFormat<Text,LouvainNodeState,LongWritable>{

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		return new LouvainVertexWriter();
	}

	
	protected class LouvainVertexWriter extends TextVertexWriter {

		@Override
		public void writeVertex(
				Vertex<Text, LouvainNodeState, LongWritable> vertex)
				throws IOException, InterruptedException {
			StringBuilder b = new StringBuilder();
			b.append(vertex.getValue().getCommunity());
			b.append("\t");
			b.append(vertex.getValue().getInternalWeight());
			b.append("\t");
			
			for (Edge<Text,LongWritable> e: vertex.getEdges()){
				b.append(e.getTargetVertexId());
				b.append(":");
				b.append(e.getValue());
				b.append(",");
			}
			b.setLength(b.length() - 1);
			
			getRecordWriter().write(vertex.getId(), new Text(b.toString()));
			
		}

	}
	
	
	
	
	
}
