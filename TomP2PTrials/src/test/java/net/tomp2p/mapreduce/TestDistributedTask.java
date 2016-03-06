package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class TestDistributedTask {
	final private static Random rnd = new Random(42L);
	final private static Logger logger = LoggerFactory.getLogger(TestDistributedTask.class);

	@Test
	public void testPut() throws IOException, InterruptedException, ClassNotFoundException {
		final PeerMapReduce[] peers = createAndAttachNodes(100, 4444);
		bootstrap(peers);
		perfectRouting(peers);

		Number160 key = Number160.createHash("VALUE1");
		PeerMapReduce peer = peers[rnd.nextInt(peers.length)];
		FutureTask start = peer.put(key, key, "VALUE1", 3).start();
		start.addListener(new BaseFutureAdapter<BaseFuture>() {

			@Override
			public void operationComplete(BaseFuture future) throws Exception {

				if (future.isSuccess()) {
					Thread.sleep(100);
					int count = 0;
					for (PeerMapReduce p : peers) {
						Data data = p.taskRPC().storage().get(new Number640(key, key, Number160.ZERO, Number160.ZERO));
						if (data != null) {
							++count;
							logger.info(count + ": " +data.object() + "");

						}
					}
					assertEquals(6, count);
				}
				
			}
		}).awaitUninterruptibly(); 
		for (PeerMapReduce p : peers) {
			p.peer().shutdown().await();
		}
	}

	@Test
	public void testGet() throws Exception {
		PeerMapReduce[] peers = createAndAttachNodes(10, 4444);
		bootstrap(peers);
		perfectRouting(peers);

		Number160 key = Number160.createHash("VALUE1");
		Number640 storageKey = new Number640(key, key, Number160.ZERO, Number160.ZERO);
		PeerMapReduce peer = peers[rnd.nextInt(peers.length)];
		FutureTask start = peer.put(key, key, "VALUE1", 3).start();
		start.awaitUninterruptibly();

		if (start.isSuccess()) {
			for (int i = 0; i < 10; ++i) {
				PeerMapReduce getter = peers[rnd.nextInt(peers.length)];
				FutureTask getTask = getter.get(key, key, new TreeMap<>()).start();
				getTask.awaitUninterruptibly();
				if (getTask.isSuccess()) {
					Map<Number640, Data> dataMap = getTask.dataMap();
					for (Number640 n : dataMap.keySet()) {
						Data data = dataMap.get(n);
						System.err.println("Iteration i[" + i + "]: data.object(): " + data.object());
						if (i >= 0 && i < 3) {
							assertEquals("VALUE1", (String) data.object());
						} else {
							assertEquals(null, data.object());
						}
					}
Thread.sleep(10000);
					// No broadcast available: do it manually
					for (PeerMapReduce p : peers) {
						// Data data = p.taskRPC().storage().get(new Number640(key, key, Number160.ZERO, Number160.ZERO));
						// if (data != null) {

						// private void informPeerConnectionActiveFlagRemoveListeners(PeerAddress sender, Number640 storageKey) {
						Method informPCAFRLMethod = MapReduceBroadcastHandler.class.getDeclaredMethod("informPeerConnectionActiveFlagRemoveListeners", PeerAddress.class, Number640.class);
						informPCAFRLMethod.setAccessible(true);
						informPCAFRLMethod.invoke(p.broadcastHandler(), getter.peer().peerAddress(), storageKey);
						// }
					}

				}
			}
		}
//		for (PeerMapReduce p : peers) {
//			p.peer().shutdown().await();
//		}

	}

	public static void perfectRouting(PeerMapReduce... peers) {
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
	public static void bootstrap(PeerMapReduce[] peers) {
		// make perfect bootstrap, the regular can take a while
		for (int i = 0; i < peers.length; i++) {
			for (int j = 0; j < peers.length; j++) {
				peers[i].peer().peerBean().peerMap().peerFound(peers[j].peer().peerAddress(), null, null, null);
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
	public static PeerMapReduce[] createAndAttachNodes(int nr, int port) throws IOException {
		PeerMapReduce[] peers = new PeerMapReduce[nr];
		for (int i = 0; i < nr; i++) {
			DHTWrapper mockDHT = Mockito.mock(DHTWrapper.class);
			if (i == 0) {
				peers[0] = new PeerMapReduce(new PeerBuilder(new Number160(RND)).ports(port).start(), mockDHT);
			} else {
				peers[i] = new PeerMapReduce(new PeerBuilder(new Number160(RND)).masterPeer(peers[0].peer()).start(), mockDHT);
			}
		}
		return peers;
	}

}
