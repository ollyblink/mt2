package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.Map;
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
	// public static int numberOfExpectedComputers = 2;

	private Peer peer;
	private MapReduceBroadcastHandler broadcastHandler;
	private TaskRPC taskRPC;
	private int waitingTime = 0;

	public PeerMapReduce(Peer peer, MapReduceBroadcastHandler broadcastHandler) {
		// PeerMapReduce.numberOfExpectedComputers = numberOfExpectedComputers;
		this.peer = peer;
		if (broadcastHandler != null) {
			this.broadcastHandler = broadcastHandler;
			this.broadcastHandler.peerMapReduce(this);
		}
		this.taskRPC = new TaskRPC(this);
	}

	public PeerMapReduce() {
		try {
			// PeerMapReduce.numberOfExpectedComputers = numberOfExpectedComputers;
			this.broadcastHandler = new MapReduceBroadcastHandler();
			this.peer = new PeerBuilder(new Number160(RND)).ports(DEFAULT_PORT).broadcastHandler(broadcastHandler).start();
			this.broadcastHandler.peerMapReduce(this);
			this.taskRPC = new TaskRPC(this);
		} catch (IOException e) {
			e.printStackTrace();
		}
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

	public PeerMapReduce(PeerBuilder peerBuilder, int waitingTime) {
		try {
			this.waitingTime = waitingTime;
			this.broadcastHandler = new MapReduceBroadcastHandler();
			this.peer = peerBuilder.broadcastHandler(broadcastHandler).start();
			this.broadcastHandler.peerMapReduce(this);
			this.taskRPC = new TaskRPC(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public PeerMapReduce(PeerBuilder peerBuilder, MapReduceBroadcastHandler broadcastHandlers) {
		try {
			// PeerMapReduce.numberOfExpectedComputers = numberOfExpectedComputers;
			this.broadcastHandler = broadcastHandler;
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
		try {
			int nextInt = new Random().nextInt(waitingTime);
			System.err.println("Waiting "+ nextInt + " ms till get");
			Thread.sleep(nextInt);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
