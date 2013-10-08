package mil.darpa.xdata.louvain.giraph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * messages sent between vertices.
 */
public class LouvainMessage implements Writable {

	private String communityId = "";
	private long communitySigmaTotal;
	private long edgeWeight;
	private String sourceId = "";
	
	public LouvainMessage(){}
	
	public LouvainMessage(String communityId,long sigmaTotal,long weight,String sourceId){
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
		communityId = WritableUtils.readString(in);
		communitySigmaTotal = in.readLong();
		edgeWeight = in.readLong();
		sourceId = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, communityId);
		//out.writeUTF(communityId);
		out.writeLong(communitySigmaTotal);
		out.writeLong(edgeWeight);
		//out.writeUTF(sourceId);
		WritableUtils.writeString(out, sourceId);
	}

	public String getCommunityId() {
		return communityId;
	}

	public void setCommunityId(String l) {
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
	
	public String getSourceId(){
		return sourceId;
	}
	
	public void setSourceId(String sourceId){
		this.sourceId = sourceId;
	}

}
