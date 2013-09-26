package mil.darpa.xdata.louvain.giraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * messages sent between vertcies.
 */
public class LouvainMessage implements Writable{

	private long communityId;
	private long communitySigmaTotal;
	private long edgeWeight;
	private long sourceId;
	
	public LouvainMessage(){}
	
	public LouvainMessage(long communityId,long sigmaTotal,long weight,long sourceId){
		this();
		this.communityId = communityId;
		this.communitySigmaTotal = sigmaTotal;
		this.edgeWeight = weight;
		this.sourceId = sourceId;
	}

	public LouvainMessage(LouvainMessage other){
		this(other.communityId,other.communitySigmaTotal,other.edgeWeight,other.sourceId);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		communityId = in.readLong();
		communitySigmaTotal = in.readLong();
		edgeWeight = in.readLong();
		sourceId = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(communityId);
		out.writeLong(communitySigmaTotal);
		out.writeLong(edgeWeight);
		out.writeLong(sourceId);
	}

	public long getCommunityId() {
		return communityId;
	}

	public void setCommunityId(long l) {
		this.communityId = l;
	}

	public long getCommunitySigmaTotal() {
		return communitySigmaTotal;
	}

	public void setCommunitySigmaTotal(long communitySigmaTotal) {
		this.communitySigmaTotal = communitySigmaTotal;
	}
	
	public void addToSigmaTotal(long partial){
		this.communitySigmaTotal += partial;
	}

	public long getEdgeWeight() {
		return edgeWeight;
	}

	public void setEdgeWeight(long edgeWeight) {
		this.edgeWeight = edgeWeight;
	}
	
	public long getSourceId(){
		return sourceId;
	}
	
	public void setSourceId(long sourceId){
		this.sourceId = sourceId;
	}

}
