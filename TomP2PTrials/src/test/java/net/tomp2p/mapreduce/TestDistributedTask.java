package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.Test;

import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.mapreduce.utils.DataStorageObject;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;
import net.tomp2p.tracker.PeerBuilderTracker;
import net.tomp2p.tracker.PeerTracker;

public class TestDistributedTask {
	final private static Random rnd = new Random(42L);

	@Test
	public void testPut() throws IOException, InterruptedException {
		PeerDHT[] peers = null;
		try {
			peers = createAndAttachPeersDHT(100, 4444);
			bootstrap(peers);
			perfectRouting(peers);
			Map<PeerDHT, TaskRPC> peersAndRPCs = new HashMap<>();

			for (PeerDHT peer : peers) {
				TaskRPC taskRPC = new TaskRPC(peer.peerBean(), peer.peer().connectionBean(), null);
				peersAndRPCs.put(peer, taskRPC);

			}

			PeerDHT peer = peers[rnd.nextInt(peers.length)];
			TaskPutDataBuilder builder = new TaskPutDataBuilder(peer, peersAndRPCs.get(peer)).storageKey(NumberUtils.allSameKey("VALUE1")).dataStorageObject(new DataStorageObject("VALUE1", 3));
			FutureTask start = builder.start();
			start.awaitUninterruptibly();
			int count = 0;
			if (start.isSuccess()) {
				for (PeerDHT p : peersAndRPCs.keySet()) {
					TaskRPC taskRPC = peersAndRPCs.get(p);
					Data data = taskRPC.storage().get(NumberUtils.allSameKey("VALUE1"));
					if (data != null) {
						++count;
						try {
							System.out.println(data.object());
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
					}
				}
			}
			assertEquals(true, count == 6);
		} finally {
			for (PeerDHT p : peers) {
				p.shutdown().await();
			}
		}

	}

	@Test
	public void testGet() throws Exception {
		PeerDHT[] peers = null;
		try {
			peers = createAndAttachPeersDHT(100, 4444);
			bootstrap(peers);
			perfectRouting(peers);
			Map<PeerDHT, TaskRPC> peersAndRPCs = new HashMap<>();

			for (PeerDHT peer : peers) {
				TaskRPC taskRPC = new TaskRPC(peer.peerBean(), peer.peer().connectionBean(), null);
				peersAndRPCs.put(peer, taskRPC);

			}

			PeerDHT putter = peers[new Random().nextInt(peers.length)];
			PeerDHT getter = peers[new Random().nextInt(peers.length)];
			TaskPutDataBuilder builder = new TaskPutDataBuilder(putter, peersAndRPCs.get(putter)).storageKey(NumberUtils.allSameKey("VALUE1")).dataStorageObject(new DataStorageObject("VALUE1", 3));
			FutureTask start = builder.start();
			start.awaitUninterruptibly();
			int count = 0;
			if (start.isSuccess()) {
				TaskGetDataBuilder getBuilder = new TaskGetDataBuilder(getter, peersAndRPCs.get(getter)).storageKey(NumberUtils.allSameKey("VALUE1")).broadcastInput(new TreeMap<>());
				FutureTask getTask = getBuilder.start();
				getTask.await();
				if (getTask.isSuccess()) {
					Map<Number640, Data> dataMap = getTask.dataMap();
					for (Number640 n : dataMap.keySet()) {
						Data data = dataMap.get(n);
						if (data != null) {
							System.err.println(data.object());
							assertEquals("VALUE1", (String) data.object());
						}
					}
				}
			}
			assertEquals(true, count == 6);
		} finally {
			for (PeerDHT p : peers) {
				p.shutdown().await();
			}
		}

	}

	public static void perfectRouting(PeerDHT... peers) {
		for (int i = 0; i < peers.length; i++) {
			for (int j = 0; j < peers.length; j++)
				peers[i].peer().peerBean().peerMap().peerFound(peers[j].peer().peerAddress(), null, null, null);
		}
		System.err.println("perfect routing done.");
	}

	static final Random RND = new Random(42L);

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

	public static PeerDHT[] createAndAttachPeersDHT(int nr, int port) throws IOException {
		PeerDHT[] peers = new PeerDHT[nr];
		for (int i = 0; i < nr; i++) {
			if (i == 0) {
				peers[0] = new PeerBuilderDHT(new PeerBuilder(new Number160(RND)).ports(port).start()).start();
			} else {
				peers[i] = new PeerBuilderDHT(new PeerBuilder(new Number160(RND)).masterPeer(peers[0].peer()).start()).start();
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
