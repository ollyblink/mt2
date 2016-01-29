package generictests;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import mapreduce.engine.broadcasting.broadcasthandlers.AbstractMapReduceBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandlerTest;
import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.natpmp.MessageResponseInterface;
import net.tomp2p.p2p.BroadcastHandler;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.tracker.PeerBuilderTracker;
import net.tomp2p.tracker.PeerTracker;

public class Example {
	private static final int PORT = 4001;
	static final Random RND = new Random(42L);

	public static void put(PeerDHT peer, Number160 key, Data value) {
		FuturePut futurePut = peer.add(key).data(value).start();
		futurePut.addListener(new BaseFutureListener<FuturePut>() {

			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					System.out.println("Put object success ");
				} else {
					System.out.println("No success on put");
				}
			}

			public void exceptionCaught(Throwable t) throws Exception {
				System.out.println("Caught exception: " + t);
			}
		});

	}

	public static void get(PeerDHT peer, Number160 key) {
		FutureGet futureGet = peer.get(key).all().start();
		futureGet.addListener(new BaseFutureListener<FutureGet>() {

			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					System.out.println("get is success: " + future.data().object());
				} else {
					System.out.println("get is NOT successful: couldn't find data: " + future.data());
				}
			}

			public void exceptionCaught(Throwable t) throws Exception {
				System.out.println("Caught exception");
				System.out.println(t.toString());
			}

		});

	}

	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		int numberOfPeers = 10;
		PeerDHT[] peers = createAndAttachPeersDHT(numberOfPeers, PORT, null);
		bootstrap(peers);
		PeerDHT master = peers[0];

		Scanner scanner = new Scanner(System.in);
		try {
			Random peerRandom = new Random();
			while (true) {
				System.out.println("Do you want to store or retrieve a value?");
				String action = scanner.nextLine();
				if (action.equalsIgnoreCase("store")) {
					System.out.println("Value to store?");
					String value = scanner.nextLine();
					put(peers[peerRandom.nextInt(peers.length)], new Number160(value.getBytes()),
							new Data(value));

				} else if (action.equalsIgnoreCase("retrieve")) {
					System.out.println("key to retrieve?");
					String key = scanner.nextLine();
					get(peers[peerRandom.nextInt(peers.length)], new Number160(key.getBytes()));
				} else {
					System.out.println("Could not recognise command");
				}
			}
		} finally {
			scanner.close();
			master.shutdown();
		}
	}

	/**
	 * Bootstraps peers to the first peer in the array.
	 * 
	 * @param peers
	 *            The peers that should be bootstrapped
	 */
	public static void bootstrap(Peer[] peers) {
		// make perfect bootstrap, the regular can take a while
		for (int i = 0; i < peers.length; i++) {
			for (int j = 0; j < peers.length; j++) {
				peers[i].peerBean().peerMap().peerFound(peers[j].peerAddress(), null, null, null);
			}
		}
	}

	public static void bootstrap(PeerDHT[] peers) {
		// make perfect bootstrap, the regular can take a while
		for (int i = 0; i < peers.length; i++) {
			for (int j = 0; j < peers.length; j++) {
				peers[i].peerBean().peerMap().peerFound(peers[j].peerAddress(), null, null, null);
			}
		}
	}

	/**
	 * Create peers with a port and attach it to the first peer in the array.
	 * 
	 * @param nr
	 *            The number of peers to be created
	 * @param port
	 *            The port that all the peer listens to. The multiplexing is done via the peer Id
	 * @return The created peers
	 * @throws IOException
	 *             IOException
	 */
	public static Peer[] createAndAttachNodes(int nr, int port) throws IOException {
		Peer[] peers = new Peer[nr];
		for (int i = 0; i < nr; i++) {
			if (i == 0) {
				peers[0] = new PeerBuilder(new Number160(RND)).ports(port).start();
			} else {
				peers[i] = new PeerBuilder(new Number160(RND)).masterPeer(peers[0]).start();
			}
		}
		return peers;
	}

	public static PeerDHT[] createAndAttachPeersDHT(int nr, int port,
			AbstractMapReduceBroadcastHandler bcHandler) throws IOException {
		return createAndAttachPeersDHT(nr, port, bcHandler, null);
	}

	public static PeerDHT[] createAndAttachPeersDHT(int nr, int port,
			AbstractMapReduceBroadcastHandler bcHandler, PeerDHT master) throws IOException {
		StructuredBroadcastHandler bcH = null;
		if (bcHandler == null) {
			bcH = new MyBroadcastHandler(1);
		} else {
			bcH = bcHandler;
		}
		// bcH = MRBroadcastHandler.create();
		PeerDHT[] peers = new PeerDHT[nr];
		if (master == null) {
			for (int i = 0; i < nr; i++) {
				if (i == 0) {
					peers[0] = new PeerBuilderDHT(
							new PeerBuilder(new Number160(RND)).ports(port).broadcastHandler(bcH).start())
									.start();
					System.err.println("Master: " + peers[0].peerID() + ", broadcast handler: " + bcH);
				} else {
					peers[i] = new PeerBuilderDHT(new PeerBuilder(new Number160(RND)).broadcastHandler(bcH)
							.masterPeer(peers[0].peer()).start()).start();
					System.err.println("Master: " + peers[0].peerID() + ", slave: " + peers[i].peerID()
							+ ", broadcast handler: " + bcH);
				}
			}
		} else {
			for (int i = 0; i < nr; i++) {
				peers[i] = new PeerBuilderDHT(new PeerBuilder(new Number160(RND)).broadcastHandler(bcH)
						.masterPeer(master.peer()).start()).start();

				System.err.println("Master: " + master.peerID() + ", slave: " + peers[i].peerID()
						+ ", broadcast handler: " + bcH);
			}
		}
		return peers;

	}

	public static PeerTracker[] createAndAttachPeersTracker(PeerDHT[] peers) throws IOException {
		PeerTracker[] peers2 = new PeerTracker[peers.length];
		for (int i = 0; i < peers.length; i++) {
			peers2[i] = new PeerBuilderTracker(peers[i].peer()).verifyPeersOnTracker(false).start();
		}
		return peers2;
	}
}
