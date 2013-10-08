package mil.darpa.xdata.louvain.giraph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;


/**
 * The state of a vertex.
 *
 */
public class LouvainNodeState implements Writable {

	private String community = "";
	private long communitySigmaTotal;
	
	// the interanal edge weight of a node
	// i.e. edges from the node to itself.
	private long internalWeight;
	
	// outgoing degree of the node
	private long nodeWeight;
	
	// 1 if the node has changed communities this cycle, otherwise 0
	private long changed;
	
	// history of total change numbers, used to determine when to halt
	private ArrayList<Long> changeHistory;
	
	
	public LouvainNodeState(){
		this.changeHistory = new ArrayList<Long>();
		this.changed = 0;
	}

	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		community = WritableUtils.readString(in);
		communitySigmaTotal = in.readLong();
		internalWeight = in.readLong();
		changed = in.readLong();
		nodeWeight = in.readLong();
		int historyLength = in.readInt();
		changeHistory = new ArrayList<Long>(2*(historyLength+1));
		for (int i = 0; i < historyLength; i++){
			changeHistory.add(in.readLong());
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, community);
		out.writeLong(communitySigmaTotal);
		out.writeLong(internalWeight);
		out.writeLong(changed);
		out.writeLong(nodeWeight);
		out.writeInt(changeHistory.size());
		for (Long i : changeHistory){
			out.writeLong(i);
		}
		
	}

	public String getCommunity() {
		return community;
	}

	public void setCommunity(String community) {
		this.community = community;
	}

	public long getCommunitySigmaTotal() {
		return communitySigmaTotal;
	}

	public void setCommunitySigmaTotal(long communitySigmaTotal) {
		this.communitySigmaTotal = communitySigmaTotal;
	}

	public long getInternalWeight() {
		return internalWeight;
	}

	public void setInternalWeight(long internalWeight) {
		this.internalWeight = internalWeight;
	}

	public long getChanged() {
		return changed;
	}

	public void setChanged(long changed) {
		this.changed = changed;
	}
	
	public ArrayList<Long> getChangeHistory(){
		return this.changeHistory;
	}

	public long getNodeWeight() {
		return nodeWeight;
	}

	public void setNodeWeight(long nodeWeight) {
		this.nodeWeight = nodeWeight;
	}

}
