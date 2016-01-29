package generictests;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import mapreduce.utils.GetOwnIpAddressTest;
import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.storage.Data;

public class TrialInternetConnection {
	private static Peer master;

	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		// final String key = "Hello World";
		Random random = new Random();
		String ipSuperPeer = "192.168.43.234";
		int port = 4000;
		GetOwnIpAddressTest.main(null);

		int peerID = 2;
		String action = "";
		final String key = "C";
		final String value = "asdfasdf";
		final String bc = "PIKKU"+random.nextLong();

		Random RND = new Random();
		if (peerID == 1) {
			master = new PeerBuilder(Number160.createHash("super peer")).ports(port).broadcastHandler(new MyBroadcastHandler(1)).start();
			PeerDHT masterDHT = new PeerBuilderDHT(master).start();

		} else {
			int port2 = port + RND.nextInt(20000) + 1;
			System.out.println("Port 2: " + port2);
			// PeerMapConfiguration p = new PeerMapConfiguration(Number160.createHash("PeerMap"));
			// p.peerVerification(false);
			final Peer myPeer = new PeerBuilder(Number160.createHash("client peer"+RND.nextLong())).ports(port2).broadcastHandler(new MyBroadcastHandler(1)).start();

			PeerDHT myPeerDHT = new PeerBuilderDHT(myPeer).start();
			// PeerAddress bootstrapServerPeerAddress = new PeerAddress(Number160.ZERO, new InetSocketAddress(InetAddress.getByName(ipSuperPeer),
			// port));
			//
			// FutureDiscover discovery = myPeer.discover().peerAddress(bootstrapServerPeerAddress).start();
			// discovery.awaitUninterruptibly();
			// if (!discovery.isSuccess()) {
			// System.err.println("A no success!");
			// }
			// System.err.println("Peer: " + discovery.reporter() + " told us about our address.");
			// bootstrapServerPeerAddress = discovery.reporter();
			// FutureBootstrap bootstrap = myPeer.bootstrap().inetAddress(InetAddress.getByName(ipSuperPeer)).ports(port).start();
			//
			// bootstrap.awaitUninterruptibly();
			//
			// if (!bootstrap.isSuccess()) {
			// System.err.println("B no success!");
			// } 
			FutureBootstrap bootstrap = myPeer.bootstrap().inetAddress(InetAddress.getByName(ipSuperPeer)).ports(port).start();

			bootstrap.awaitUninterruptibly();
			if (!bootstrap.isSuccess()) {
				System.err.println("B no success!");
			}
 
			// myPeer.peerBean().peerMap().peerFound(bootstrapServerPeerAddress, null, null, null);
			// System.out.println("Bootstrapped to:" +p.inetAddress() + " " + p.tcpPort() + " " + p.udpPort());
			// }
			// System.out.println("Peers:" + myPeer.peerBean().peerMap());
			// System.out.println("Peers: " + myPeer.peerBean().peerMap());
			//
			if (action.equals("")) {
				Number160 hash = Number160.createHash("Hello");
				final NavigableMap<Number640, Data> dataMap = new TreeMap<Number640, Data>();
				dataMap.put(new Number640(hash, hash, hash, hash), new Data(bc));
				myPeer.broadcast(hash).dataMap(dataMap).start();

				// StructuredBroadcastHandler d = (StructuredBroadcastHandler) myPeer.broadcastRPC().broadcastHandler();
				// System.out.println(d.broadcastCounter());
				// while(d.broadcastCounter() <= 1){
				// System.out.println("Here");
				// Thread.sleep(200);
				// }
			}
			// //
			// if (action.equals("PUT")) {
			//
			// FuturePut putFuture = myPeerDHT.add(Number160.createHash(key)).data(new Data(value)).start();
			// putFuture.addListener(new BaseFutureListener<FuturePut>() {
			//
			// @Override
			// public void operationComplete(FuturePut future) throws Exception {
			// if (future.isSuccess()) {
			// System.out.println("Success on put");
			// } else {
			// System.out.println("C No success");
			// }
			// }
			//
			// @Override
			// public void exceptionCaught(Throwable t) throws Exception {
			// t.printStackTrace();
			// }
			//
			// });
			// }
			// if (action.equals("GET")) {
			// FutureGet futureDHT = myPeerDHT.get(Number160.createHash(key)).all().start();
			// futureDHT.addListener(new BaseFutureListener<FutureGet>() {
			//
			// @Override
			// public void operationComplete(FutureGet future) throws Exception {
			// if (future.isSuccess()) {
			// System.out.println(future.data().object());
			// } else {
			// System.out.println("C No success");
			// }
			// }
			//
			// @Override
			// public void exceptionCaught(Throwable t) throws Exception {
			// t.printStackTrace();
			// }
			//
			// });
			// // Data data = futureDHT.data();
			// // if (data == null) {
			// // throw new RuntimeException("Address not available in DHT.");
			// // }
			// // InetSocketAddress inetSocketAddress = (InetSocketAddress) data.object();
			// // System.err.println("returned " + inetSocketAddress);
			// }
			// myPeer.shutdown();
		}
	}
}
