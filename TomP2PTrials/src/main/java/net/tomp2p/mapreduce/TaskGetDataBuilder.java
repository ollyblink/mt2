package net.tomp2p.mapreduce;

import java.util.NavigableMap;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.dht.FutureSend;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.VotingSchemeDHT;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.mapreduce.utils.DataStorageObject;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.peers.Number640;

public class TaskGetDataBuilder extends DefaultConnectionConfiguration {

	// private boolean isForceTCP = false;
	private Number640 storageKey;
	// private DataStorageObject dataStorageObject;
	private NavigableMap<Number640, byte[]> broadcastInput;
	protected final PeerDHT peer;

	protected RoutingConfiguration routingConfiguration;
	protected RequestP2PConfiguration requestP2PConfiguration;
	protected FutureChannelCreator futureChannelCreator;

	// private TaskDataBuilder self;
	private TaskRPC taskRPC;

	public TaskGetDataBuilder(PeerDHT peer, TaskRPC taskRPC) {
		this.peer = peer;
		this.taskRPC = taskRPC;

	}

	public TaskGetDataBuilder broadcastInput(NavigableMap<Number640, byte[]> broadcastInput) {
		this.broadcastInput = broadcastInput;
		return this;
	}

	public NavigableMap<Number640, byte[]> broadcastInput() {
		return broadcastInput;
	}
	// public void self(TaskDataBuilder self) {
	// this.self = self;
	// }

	public FutureTask start() {
		if (peer.peer().isShutdown()) {
			return null;
		}
		if (routingConfiguration == null) {
			routingConfiguration = new RoutingConfiguration(5, 10, 2);
		}
		if (requestP2PConfiguration == null) {
			requestP2PConfiguration = new RequestP2PConfiguration(3, 5, 3);
		}
		int size = peer.peer().peerBean().peerMap().size() + 1;
		requestP2PConfiguration = requestP2PConfiguration.adjustMinimumResult(size);
		if (futureChannelCreator == null || (futureChannelCreator.channelCreator() != null && futureChannelCreator.channelCreator().isShutdown())) {
			futureChannelCreator = peer.peer().connectionBean().reservation().create(routingConfiguration, requestP2PConfiguration, this);
		}
		final FutureTask futureTask = new FutureTask();
		return new DistributedTask(peer.peer().distributedRouting(), taskRPC).getTaskData(this, futureTask);
	}

	/**
	 * @return The configuration for the routing options
	 */
	public RoutingConfiguration routingConfiguration() {
		return routingConfiguration;
	}

	/**
	 * @param routingConfiguration
	 *            The configuration for the routing options
	 * @return This object
	 */
	public TaskGetDataBuilder routingConfiguration(final RoutingConfiguration routingConfiguration) {
		this.routingConfiguration = routingConfiguration;
		return this;
	}

	/**
	 * @return The P2P request configuration options
	 */
	public RequestP2PConfiguration requestP2PConfiguration() {
		return requestP2PConfiguration;
	}

	/**
	 * @param requestP2PConfiguration
	 *            The P2P request configuration options
	 * @return This object
	 */
	public TaskGetDataBuilder requestP2PConfiguration(final RequestP2PConfiguration requestP2PConfiguration) {
		this.requestP2PConfiguration = requestP2PConfiguration;
		return this;
	}

	/**
	 * @return The future of the created channel
	 */
	public FutureChannelCreator futureChannelCreator() {
		return futureChannelCreator;
	}

	/**
	 * @param futureChannelCreator
	 *            The future of the created channel
	 * @return This object
	 */
	public TaskGetDataBuilder futureChannelCreator(FutureChannelCreator futureChannelCreator) {
		this.futureChannelCreator = futureChannelCreator;
		return this;
	}

	public TaskGetDataBuilder storageKey(Number640 storageKey) {
		this.storageKey = storageKey;
		return this;
	}

	public Number640 storageKey() {
		return this.storageKey;
	}

}
