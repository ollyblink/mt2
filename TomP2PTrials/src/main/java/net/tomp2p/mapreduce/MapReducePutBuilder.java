package net.tomp2p.mapreduce;

import net.tomp2p.mapreduce.utils.MapReduceValue;
import net.tomp2p.peers.Number160;

public class MapReducePutBuilder extends BaseMapReduceBuilder<MapReducePutBuilder> {

	private MapReduceValue data;
	public String execId; // Only used for testing purposes

	public MapReducePutBuilder(PeerMapReduce peerMapReduce, Number160 locationKey, Number160 domainKey) {
		super(peerMapReduce, locationKey, domainKey);
		self(this);
	}

	public FutureTask start() {
		return new DistributedTask(peerMapReduce.peer().distributedRouting(), peerMapReduce.taskRPC()).putTaskData(this, super.start());
	}

	public MapReducePutBuilder data(Object value, int nrOfExecutions) {
		this.data = new MapReduceValue(value, nrOfExecutions);
		return this;
	}

	public MapReduceValue data() {
		return this.data;
	}

}
