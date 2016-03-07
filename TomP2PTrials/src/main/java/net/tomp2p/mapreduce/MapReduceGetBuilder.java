package net.tomp2p.mapreduce;

import java.util.NavigableMap;
import java.util.TreeMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MapReduceGetBuilder extends BaseMapReduceBuilder<MapReduceGetBuilder> {

	private NavigableMap<Number640, byte[]> broadcastInput;

	public MapReduceGetBuilder(PeerMapReduce peerMapReduce, Number160 locationKey, Number160 domainKey) {
		super(peerMapReduce, locationKey, domainKey);
		self(this);
	}

	public MapReduceGetBuilder broadcastInput(NavigableMap<Number640, Data> broadcastInput) {
		this.broadcastInput = convertDataToByteArray(broadcastInput);
		return this;
	}

	public NavigableMap<Number640, byte[]> broadcastInput() {
		return broadcastInput;
	}

	public FutureTask start() {
		return new DistributedTask(peerMapReduce.peer().distributedRouting(), peerMapReduce.taskRPC()).getTaskData(this, super.start());
	}

	public static NavigableMap<Number640, byte[]> convertDataToByteArray(NavigableMap<Number640, Data> input) {
		NavigableMap<Number640, byte[]> convertedBroadcastInput = new TreeMap<>();
		for (Number640 key : input.keySet()) {
			convertedBroadcastInput.put(key, input.get(key).toBytes());
		}
		return convertedBroadcastInput;
	}

	public static NavigableMap<Number640, Data> reconvertByteArrayToData(NavigableMap<Number640, byte[]> input) {
		NavigableMap<Number640, Data> convertedBroadcastInput = new TreeMap<>();
		for (Number640 key : input.keySet()) {
			convertedBroadcastInput.put(key, new Data(input.get(key)));
		}
		return convertedBroadcastInput;
	}
}
