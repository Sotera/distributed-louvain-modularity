package mil.darpa.xdata.louvain.giraph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Performs the BSP portion of the distributed louvain algorithim.
 * 
 * The computation is completed as a series of repeated steps, movement is
 * restricted to ~half of the nodes on each cycle so a full pass requires two
 * cycles
 * 
 * step 1. each vertex recieves community values from its community hub, and
 * sends its community to its neighbors step 2. each vertex determines if it
 * should move to a neighboring community or not, and sends its information to
 * its community hub. step 3. each community hub re-calcuates community totals,
 * and sends the updates to each community memeber.
 * 
 * When the number of nodes that change communities stops decreassing for 4
 * cycles, or when the number of nodes that change reaches 0 the computation
 * ends.
 * 
 * 
 * 
 * @author Eric Kimbrel - Sotera Defense
 * 
 */
public class LouvainVertex extends Vertex<Text, LouvainNodeState, LongWritable, LouvainMessage> {

	// constants used to register and lookup aggregators
	public static final String CHANGE_AGG = "change_aggregator";
	public static final String TOTAL_EDGE_WEIGHT_AGG = "total_edge_weight_aggregator";

	// It may be the case that splitting the aggregators to multiple aggreators
	// will improve performance, set actual.Q.aggregators to set the number,
	// defaults to 1
	public static final String ACTUAL_Q_AGG = "actual_q_aggregator";

//	public static int getNumQAggregators(Configuration conf) {
//		return conf.getInt("actual.Q.aggregators", 1);
//	}

	private void aggregateQ(Double q) {
//		int aggregators = getNumQAggregators(getConf());
//		if (0 < aggregators) {
//			int modId = (int) this.getId().get() % aggregators;
			aggregate(ACTUAL_Q_AGG, new DoubleWritable(q));
//		}
	}

	@Override
	public void compute(Iterable<LouvainMessage> messages) throws IOException {

		long superstep = getSuperstep();
		int minorstep = (int) (superstep % 3); // the step in this iteration
		int iteration = (int) (superstep / 3); // the current iteration, two
												// iterations make a full pass.

		// count the total edge weight of the graph on the first super step only
		if (superstep == 0) {
			aggregate(TOTAL_EDGE_WEIGHT_AGG, new LongWritable(getValue().getNodeWeight() + getValue().getInternalWeight()));
		}

		// nodes that have no edges send themselves a message on the step 0
		if (superstep == 0 && !getEdges().iterator().hasNext()) {
			this.sendMessage(this.getId(), new LouvainMessage());
			voteToHalt();
			return;
		}
		// nodes that have no edges aggregate their Q value and exit computation
		// on step 1
		else if (superstep == 1 && !getEdges().iterator().hasNext()) {
			double q = calculateActualQ(new ArrayList<LouvainMessage>());
			aggregateQ(q);
			voteToHalt();
			return;
		}

		// at the start of each full pass check to see if progress is still
		// being made, if not halt
		if (minorstep == 1 && iteration > 0 && iteration % 2 == 0) {
			getValue().setChanged(0); // change count is per pass
			long totalChange = ((LongWritable) getAggregatedValue(CHANGE_AGG)).get();
			getValue().getChangeHistory().add(totalChange);

			// if halting aggregate q value and replace node edges with
			// community edges (for next stage in pipeline)
			if (LouvainMasterCompute.decideToHalt(getValue().getChangeHistory(), getConf())) {
				double q = calculateActualQ(messages);
				replaceNodeEdgesWithCommunityEdges(messages);
				aggregateQ(q);
				return;
				// note: we did not vote to halt, MasterCompute will halt
				// computation on next step
			}
		}

		try {
			switch (minorstep) {

			case 0:
				getAndSendCommunityInfo(messages);

				// if the next step well require a progress check,
				// aggregate the number of nodes who have changed community.
				if (iteration > 0 && iteration % 2 == 0) {
					aggregate(CHANGE_AGG, new LongWritable(getValue().getChanged()));
				}

				break;
			case 1:
				calculateBestCommunity(messages, iteration);
				break;
			case 2:
				updateCommunities(messages);
				break;
			default:
				throw new IllegalArgumentException("Invalid minorstep: " + minorstep);
			}
		} finally {
			voteToHalt();
		}

	}

	/**
	 * Get the total edge weight of the graph.
	 * 
	 * @return 2*the total graph weight.
	 */
	private long getTotalEdgeWeight() {
		long m = ((LongWritable) getAggregatedValue(TOTAL_EDGE_WEIGHT_AGG)).get();
		return m;
	}

	/**
	 * Each vertex will recieve its own communities sigma_total (if updated),
	 * and then send its currenty community info to each of its neighbors.
	 * 
	 * @param messages
	 */
	private void getAndSendCommunityInfo(Iterable<LouvainMessage> messages) {
		LouvainNodeState state = this.getValue();
		// set new community information.
		if (getSuperstep() > 0) {
			Iterator<LouvainMessage> it = messages.iterator();
			if (!it.hasNext()) {
				throw new IllegalStateException("No community info recieved in getAndSendCommunityInfo! Superstep: " + getSuperstep() + " id: " + this.getId());
			}
			LouvainMessage inMess = it.next();
			if (it.hasNext()) {
				throw new IllegalStateException("More than one community info packets recieved in getAndSendCommunityInfo! Superstep: " + getSuperstep() + " id: " + this.getId());
			}
			state.setCommunity(inMess.getCommunityId());
			state.setCommunitySigmaTotal(inMess.getCommunitySigmaTotal());
		}

		// send community info to all neighbors
		for (Edge<Text, LongWritable> edge : getEdges()) {
			LouvainMessage outMess = new LouvainMessage();
			outMess.setCommunityId(state.getCommunity());
			outMess.setCommunitySigmaTotal(state.getCommunitySigmaTotal());
			outMess.setEdgeWeight(edge.getValue().get());
			outMess.setSourceId(getId().toString());
			this.sendMessage(edge.getTargetVertexId(), outMess);
		}

	}

	/**
	 * Based on community of each of its neighbors, each vertex determimnes if
	 * it should retain its currenty community or swtich to a neighboring
	 * communinity.
	 * 
	 * At the end of this step a message is sent to the nodes community hub so a
	 * new community sigma_total can be calculated.
	 * 
	 * @param messages
	 * @param iteration
	 */
	private void calculateBestCommunity(Iterable<LouvainMessage> messages, int iteration) {

		LouvainNodeState state = getValue();

		// group messages by communities.
		HashMap<String, LouvainMessage> communityMap = new HashMap<String, LouvainMessage>();
		for (LouvainMessage message : messages) {

			String communityId = message.getCommunityId();
			long weight = message.getEdgeWeight();
			LouvainMessage newmess = new LouvainMessage(message);

			if (communityMap.containsKey(communityId)) {
				LouvainMessage m = communityMap.get(communityId);
				m.setEdgeWeight(m.getEdgeWeight() + weight);
			} else {
				communityMap.put(communityId, newmess);
			}
		}

		// calculate change in Q for each potential community
		String bestCommunityId = getValue().getCommunity();
		String startingCommunityId = bestCommunityId;
		BigDecimal maxDeltaQ = new BigDecimal("0.0");
		for (Map.Entry<String, LouvainMessage> entry : communityMap.entrySet()) {
			BigDecimal deltaQ = q(startingCommunityId, entry.getValue().getCommunityId(), entry.getValue().getCommunitySigmaTotal(), entry.getValue().getEdgeWeight(), state.getNodeWeight(), state.getInternalWeight());
			if (deltaQ.compareTo(maxDeltaQ) > 0 || (deltaQ.equals(maxDeltaQ) && entry.getValue().getCommunityId().compareTo(bestCommunityId) < 0)) {
				bestCommunityId = entry.getValue().getCommunityId();
				maxDeltaQ = deltaQ;
			}
		}

		// ignore switches based on iteration (prevent certain cycles)
		if ((state.getCommunity().compareTo(bestCommunityId) > 0 && iteration % 2 == 0) || (state.getCommunity().compareTo(bestCommunityId) < 0 && iteration % 2 != 0)) {
			bestCommunityId = state.getCommunity();
			// System.out.println("Iteration: "+iteration+" Node: "+getId()+" held stable to prevent cycle");
		}

		// update community and change count
		if (!state.getCommunity().equals(bestCommunityId)) {
			// long old = state.getCommunity();
			LouvainMessage c = communityMap.get(bestCommunityId);
			if (!bestCommunityId.equals(c.getCommunityId())) {
				throw new IllegalStateException("Community mapping contains wrong Id");
			}
			state.setCommunity(c.getCommunityId());
			state.setCommunitySigmaTotal(c.getCommunitySigmaTotal());
			state.setChanged(1L);
			// System.out.println("Iteration: "+iteration+" Node: "+getId()+" changed from "+old+" -> "+state.getCommunity()+" dq: "+maxDeltaQ);
		}

		// send our node weight to the community hub to be summed in next
		// superstep
		this.sendMessage(new Text(state.getCommunity()), new LouvainMessage(state.getCommunity(), state.getNodeWeight() + state.getInternalWeight(), 0, getId().toString()));
	}

	/**
	 * determine the change in q if a node were to move to the given community.
	 * 
	 * @param currCommunityId
	 * @param testCommunityId
	 * @param testSigmaTot
	 * @param edgeWeightInCommunity
	 *            (sum of weight of edges from this ndoe to target community)
	 * @param nodeWeight
	 *            (the node degree)
	 * @param internalWeight
	 * @return
	 */
	private BigDecimal q(String currCommunityId, String testCommunityId, long testSigmaTot, long edgeWeightInCommunity, long nodeWeight, long internalWeight) {
		boolean isCurrentCommunity = (currCommunityId.equals(testCommunityId));
		BigDecimal M = new BigDecimal(Long.toString(getTotalEdgeWeight()));
		long k_i_in_L = (isCurrentCommunity) ? edgeWeightInCommunity + internalWeight : edgeWeightInCommunity;
		BigDecimal k_i_in = new BigDecimal(Long.toString(k_i_in_L));
		BigDecimal k_i = new BigDecimal(Long.toString(nodeWeight + internalWeight));
		BigDecimal sigma_tot = new BigDecimal(Long.toString(testSigmaTot));
		if (isCurrentCommunity) {
			sigma_tot = sigma_tot.subtract(k_i);
		}
		// diouble sigma_tot_temp = (isCurrentCommunity) ? testSigmaTot - k_i :
		// testSigmaTot;
		BigDecimal deltaQ = new BigDecimal("0.0");
		if (!(isCurrentCommunity && sigma_tot.equals(deltaQ))) {
			BigDecimal dividend = k_i.multiply(sigma_tot);
			int scale = 20;
			deltaQ = k_i_in.subtract(dividend.divide(M, scale, RoundingMode.HALF_DOWN));

		}
		return deltaQ;
	}

	/**
	 * Each commuity hub aggregates the values from each of its memebers to
	 * update the nodes sigma total, and then sends this back to each of its
	 * members.
	 * 
	 * @param messages
	 */
	private void updateCommunities(Iterable<LouvainMessage> messages) {
		// sum all community contributions
		LouvainMessage sum = new LouvainMessage();
		sum.setCommunityId(getId().toString());
		sum.setCommunitySigmaTotal(0);
		for (LouvainMessage m : messages) {
			sum.addToSigmaTotal(m.getCommunitySigmaTotal());
		}

		// send community back out to all community members
		for (LouvainMessage m : messages) {
			this.sendMessage(new Text(m.getSourceId()), sum);
		}
	}

	/**
	 * Calculate this nodes contribution for the actual q value of the graph.
	 */
	private double calculateActualQ(Iterable<LouvainMessage> messages) {
		// long start = System.currentTimeMillis();
		LouvainNodeState state = getValue();
		long k_i_in = state.getInternalWeight();
		for (LouvainMessage m : messages) {
			if (m.getCommunityId().equals(state.getCommunity())) {
				try {
					k_i_in += this.getEdgeValue(new Text(m.getSourceId())).get();
				} catch (NullPointerException e) {
					throw new IllegalStateException("Node: " + getId() + " does not have edge: " + m.getSourceId() + "  check that the graph is bi-directional.");
				}
			}
		}
		long sigma_tot = getValue().getCommunitySigmaTotal();
		long M = this.getTotalEdgeWeight();
		long k_i = getValue().getNodeWeight() + getValue().getInternalWeight();

		double q = ((((double) k_i_in) / M) - (((double) (sigma_tot * k_i)) / Math.pow(M, 2)));
		q = (q < 0) ? 0 : q;

		// long end = System.currentTimeMillis();
		// System.out.println("calculated actual q in :"+(end-start));
		return q;
	}

	/**
	 * Replace each edge to a neighbor with an edge to that neigbors community
	 * instead. Done just before exiting computation. In the next state of the
	 * piple line this edges are aggregated and all communities are represented
	 * as single nodes. Edges from the community to itself are tracked be the
	 * ndoes interal weight.
	 * 
	 * @param messages
	 */
	private void replaceNodeEdgesWithCommunityEdges(Iterable<LouvainMessage> messages) {

		// group messages by communities.
		HashMap<String, LouvainMessage> communityMap = new HashMap<String, LouvainMessage>();
		for (LouvainMessage message : messages) {

			String communityId = message.getCommunityId();

			if (communityMap.containsKey(communityId)) {
				LouvainMessage m = communityMap.get(communityId);
				m.setEdgeWeight(m.getEdgeWeight() + message.getEdgeWeight());
			} else {
				LouvainMessage newmess = new LouvainMessage(message);
				communityMap.put(communityId, newmess);
			}
		}

		ArrayList<Edge<Text, LongWritable>> edges = new ArrayList<Edge<Text, LongWritable>>(communityMap.size() + 1);
		for (Map.Entry<String, LouvainMessage> entry : communityMap.entrySet()) {
			edges.add(EdgeFactory.create(new Text(entry.getKey()), new LongWritable(entry.getValue().getEdgeWeight())));
		}
		this.setEdges(edges);
	}

}
