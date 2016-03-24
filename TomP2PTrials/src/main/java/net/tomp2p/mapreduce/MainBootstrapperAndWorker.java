package net.tomp2p.mapreduce;

import java.net.InetAddress;
import java.util.Random;

import com.google.common.net.InetAddresses;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;

public class MainBootstrapperAndWorker {

	private static int peerCounter = 2;

	public static void main(String[] args) throws Exception {
//		String myIP = "192.168.";

		ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_TCP_IDLE_MILLIS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP = Integer.MAX_VALUE;
//		ConnectionBean.DEFAULT_UDP_IDLE_MILLIS = Integer.MAX_VALUE;
//		ChannelServerConfiguration c;
		// int nrOfFiles = 5;
		PeerConnectionCloseListener.WAITING_TIME = Integer.MAX_VALUE; // Should be less than shutdown time (reps*sleepingTime)
		//
		String bootstrapperToConnectTo = "130.60.156.102";
		int bootstrapperPortToConnectTo = 4004;
		MapReduceBroadcastHandler broadcastHandler = new MapReduceBroadcastHandler();

		Number160 id = new Number160(peerCounter);
		PeerMapConfiguration pmc = new PeerMapConfiguration(id);
		pmc.peerNoVerification();
		PeerMap pm = new PeerMap(pmc);
		Peer peer = new PeerBuilder(id).peerMap(pm).ports(bootstrapperPortToConnectTo).broadcastHandler(broadcastHandler).start();
//		Bindings b = new Bindings().addAddress(InetAddresses.forString(myIP));

		boolean isBootStrapper = (peerCounter == 2);

		if (!isBootStrapper) {
			peer.bootstrap().inetAddress(InetAddress.getByName(bootstrapperToConnectTo)).ports(bootstrapperPortToConnectTo).start().awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureBootstrap>() {

				@Override
				public void operationComplete(FutureBootstrap future) throws Exception {
					if (future.isSuccess()) {
						System.err.println("successfully bootstrapped to " + bootstrapperToConnectTo + "/" + bootstrapperPortToConnectTo);
					} else {
						System.err.println("No success on bootstrapping: fail reason: " + future.failedReason());
					}
				}

			});
		}
		new PeerMapReduce(peer, broadcastHandler);
 	}
}
