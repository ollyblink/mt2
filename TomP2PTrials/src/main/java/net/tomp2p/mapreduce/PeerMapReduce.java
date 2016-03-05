package net.tomp2p.mapreduce;

import java.util.NavigableMap;
import java.util.Random;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class PeerMapReduce {
	private static final Random RND = new Random();
	private Peer peer;
	private MapReduceBroadcastHandler broadcastHandler;
	private TaskRPC taskRPC;

	public PeerMapReduce(Peer peer, DHTWrapper dht) {
		this.peer = peer;
		this.broadcastHandler = new MapReduceBroadcastHandler(dht);
		this.taskRPC = new TaskRPC(peer.peerBean(), peer.connectionBean(), broadcastHandler);
	}

	public MapReducePutBuilder put(Number160 locationKey, Number160 domainKey, Object value, int nrOfExecutions) {
		return new MapReducePutBuilder(this, locationKey, domainKey).data(value, nrOfExecutions);
	}

	public MapReduceGetBuilder get(Number160 locationKey, Number160 domainKey, NavigableMap<Number640, Data> broadcastInput) {
		return new MapReduceGetBuilder(this, locationKey, domainKey).broadcastInput(broadcastInput);
	}

	public Peer peer() {
		return this.peer;
	}

	public MapReduceBroadcastHandler broadcastHandler() {
		return this.broadcastHandler;
	}

	public void broadcast(NavigableMap<Number640, Data> input) {
		this.peer.broadcast(new Number160(RND)).dataMap(input).start();
	}

	public TaskRPC taskRPC() {
		return this.taskRPC;
	}

}
