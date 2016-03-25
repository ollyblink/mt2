package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Random;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class PeerMapReduce {
	private static final int DEFAULT_PORT = 4444;
	private static final Random RND = new Random();
	private Peer peer;
	private MapReduceBroadcastHandler broadcastHandler;
	private TaskRPC taskRPC;

	public PeerMapReduce(Peer peer, MapReduceBroadcastHandler broadcastHandler) {
		this.peer = peer;
		if (broadcastHandler != null) {
			this.broadcastHandler = broadcastHandler;
			this.broadcastHandler.peerMapReduce(this);
		}
		this.taskRPC = new TaskRPC(this);
	}

	public PeerMapReduce() throws IOException {
		this.broadcastHandler = new MapReduceBroadcastHandler();
		this.peer = new PeerBuilder(new Number160(RND)).ports(DEFAULT_PORT).broadcastHandler(broadcastHandler).start();
		this.broadcastHandler.peerMapReduce(this);
		this.taskRPC = new TaskRPC(this);
	}

	public PeerMapReduce(PeerBuilder peerBuilder) {
		try {
			this.broadcastHandler = new MapReduceBroadcastHandler();
			this.peer = peerBuilder.broadcastHandler(broadcastHandler).start();
			this.broadcastHandler.peerMapReduce(this);
			this.taskRPC = new TaskRPC(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
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

	public TaskRPC taskRPC() {
		return this.taskRPC;
	}

}
