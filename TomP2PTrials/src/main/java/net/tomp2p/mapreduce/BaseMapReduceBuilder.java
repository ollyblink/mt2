package net.tomp2p.mapreduce;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public abstract class BaseMapReduceBuilder<K extends BaseMapReduceBuilder<K>> extends DefaultConnectionConfiguration {

	protected RoutingConfiguration routingConfiguration;
	protected RequestP2PConfiguration requestP2PConfiguration;
	protected FutureChannelCreator futureChannelCreator;

	private K self;
	private Number160 domainKey;
	private Number160 locationKey;
	protected PeerMapReduce peerMapReduce;

	public BaseMapReduceBuilder(PeerMapReduce peerMapReduce, Number160 locationKey, Number160 domainKey) {
		this.peerMapReduce = peerMapReduce;
		this.locationKey = locationKey;
		this.domainKey = domainKey;
		this.idleTCPMillis(Integer.MAX_VALUE);
		this.connectionTimeoutTCPMillis(Integer.MAX_VALUE);
		this.slowResponseTimeoutSeconds(Integer.MAX_VALUE);
//		this.idleUDPMillis(10000);
	}

	protected void self(K self) {
		this.self = self;
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
	public K routingConfiguration(final RoutingConfiguration routingConfiguration) {
		this.routingConfiguration = routingConfiguration;
		return self;
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
	public K requestP2PConfiguration(final RequestP2PConfiguration requestP2PConfiguration) {
		this.requestP2PConfiguration = requestP2PConfiguration;
		return self;
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
	public K futureChannelCreator(FutureChannelCreator futureChannelCreator) {
		this.futureChannelCreator = futureChannelCreator;
		return self;
	}

	public Number160 locationKey() {
		return this.locationKey;
	}

	public K locationKey(Number160 locationKey) {
		this.locationKey = locationKey;
		return self;
	}

	public Number160 domainKey() {
		return this.domainKey;
	}

	public K domainKey(Number160 domainKey) {
		this.domainKey = domainKey;
		return self;
	}
	// public K storageKey(Number640 storageKey) {
	// this.storageKey = storageKey;
	// return self;
	// }
	//
	// public Number640 storageKey() {
	// return this.storageKey;
	// }

	public FutureTask start() {
		if (this.peerMapReduce.peer().isShutdown()) {
			return null;
		}
		if (routingConfiguration == null) {
			routingConfiguration = new RoutingConfiguration(5, 10, 2);
		}
		if (requestP2PConfiguration == null) {
			requestP2PConfiguration = new RequestP2PConfiguration(2, 5, 5);
		}
		int size = peerMapReduce.peer().peerBean().peerMap().size() + 1; 
		requestP2PConfiguration = requestP2PConfiguration.adjustMinimumResult(size);
		if (futureChannelCreator == null || (futureChannelCreator.channelCreator() != null && futureChannelCreator.channelCreator().isShutdown())) {
			futureChannelCreator = peerMapReduce.peer().connectionBean().reservation().create(routingConfiguration, requestP2PConfiguration, this);
		}
		final FutureTask futureTask = new FutureTask();
		return futureTask;
	}
}