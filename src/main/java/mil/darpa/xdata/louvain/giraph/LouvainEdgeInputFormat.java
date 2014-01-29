package mil.darpa.xdata.louvain.giraph;

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class LouvainEdgeInputFormat extends TextEdgeInputFormat<Text,LongWritable>{

	
	@Override
	public EdgeReader<Text, LongWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		
		String duplicator = this.getConf().get("louvain.io.reverseEdgeDuplicator");
		boolean useDuplicator =  (null != duplicator) ? Boolean.parseBoolean(duplicator) : false;
		String delimiter = this.getConf().get("louvain.io.edgeDelimiter");
		if (null == delimiter) delimiter = "\t";  //default to using tabs
		EdgeReader<Text,LongWritable> baseReader = new LouvainEdgeReader(delimiter);
		return (useDuplicator) ? new ReverseEdgeDuplicator<Text, LongWritable>( baseReader) : baseReader;
	}



	
	 class Triple{
		public Text src;
		public Text target;
		public LongWritable value;
	}

	
	public class LouvainEdgeReader extends TextEdgeReaderFromEachLineProcessed<Triple> {

		String delimiter;
		
		public LouvainEdgeReader(String delimiter){
			this.delimiter = delimiter;
		}
		
		@Override
		protected Triple preprocessLine(Text line)
				throws IOException {
			String[] tokens = line.toString().trim().split(delimiter);
			Triple v = new Triple();
			v.src = new Text(tokens[0]);
			v.target = new Text(tokens[1]);
			if (tokens.length < 3){
				v.value = new LongWritable(1L);
			}
			else{
				v.value = new LongWritable(Long.parseLong(tokens[2]));
			}
			return v;
		}

		@Override
		protected Text getTargetVertexId(Triple v) throws IOException {
			return v.target;
		}

		@Override
		protected Text getSourceVertexId(Triple v) throws IOException {
			return v.src;
		}

		@Override
		protected LongWritable getValue(Triple v) throws IOException {
			return v.value;
		}
	
	}


}