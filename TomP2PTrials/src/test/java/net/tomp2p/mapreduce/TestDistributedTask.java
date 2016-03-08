package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.examplejob.TestExampleJob;
import net.tomp2p.mapreduce.utils.MapReduceValue;
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
		final PeerMapReduce[] peers = TestExampleJob.createAndAttachNodes(100, 4444);
		TestExampleJob.bootstrap(peers);
		TestExampleJob.perfectRouting(peers);

		Number160 key = Number160.createHash("VALUE1");
		PeerMapReduce peer = peers[rnd.nextInt(peers.length)];
		FutureTask start = peer.put(key, key, "VALUE1", 3).start();
		Thread.sleep(1000);
		start.addListener(new BaseFutureAdapter<BaseFuture>() {

			@Override
			public void operationComplete(BaseFuture future) throws Exception {

				if (future.isSuccess()) {
					int count = 0;
					for (PeerMapReduce p : peers) {
						Data data = p.taskRPC().storage().get(new Number640(key, key, Number160.ZERO, Number160.ZERO));
						if (data != null) {
							++count;
							logger.info(count + ": " + data.object() + "");

						}
					}
					assertEquals(6, count);
				}

			}
		}).awaitUninterruptibly();
		Thread.sleep(1000);
		for (PeerMapReduce p : peers) {
			p.peer().shutdown().await();
		}
	}

	@Test
	public void testGet() throws InterruptedException, NoSuchFieldException, SecurityException, ClassNotFoundException, IOException, IllegalArgumentException, IllegalAccessException {
		int nrOfAcquires = 3;
		int nrOfAcquireTries = 3;
		PeerMapReduce[] peers = null;
		try {
			peers = TestExampleJob.createAndAttachNodes(100, 4444);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		TestExampleJob.bootstrap(peers);
		TestExampleJob.perfectRouting(peers);
		final PeerMapReduce[] p2 = peers;

		Number160 key = Number160.createHash("VALUE1");
		Number640 storageKey = new Number640(key, key, Number160.ZERO, Number160.ZERO);
		PeerMapReduce peer = peers[rnd.nextInt(peers.length)];
		FutureTask start = peer.put(key, key, "VALUE1", nrOfAcquires).start();
		start.awaitUninterruptibly();
		final List<Integer> counts = Collections.synchronizedList(new ArrayList<>());
		List<FutureDone<Void>> all = new ArrayList<>();
		if (start.isSuccess()) {
			for (int i = 0; i < nrOfAcquireTries; ++i) {
				int i2 = i;
				PeerMapReduce getter = peers[rnd.nextInt(peers.length)];
				System.err.println("ALL SIZE: " + all.size());
				all.add(getter.get(key, key, new TreeMap<>()).start().addListener(new BaseFutureAdapter<FutureTask>() {

					@Override
					public void operationComplete(FutureTask future) throws Exception {
						if (future.isSuccess()) {
							Map<Number640, Data> dataMap = future.dataMap();
							for (Number640 n : dataMap.keySet()) {
								Data data = dataMap.get(n);
								if (data != null) {
									counts.add(new Integer(1));
									System.err.println("Iteration [" + i2 + "] acquired data");
								}
							}
							// No broadcast available: do it manually
							for (PeerMapReduce p : p2) {
								Data data = p.taskRPC().storage().get(new Number640(key, key, Number160.ZERO, Number160.ZERO));
								if (data != null) {
									Method informPCAFRLMethod = MapReduceBroadcastHandler.class.getDeclaredMethod("informPeerConnectionActiveFlagRemoveListeners", PeerAddress.class, Number640.class);
									informPCAFRLMethod.setAccessible(true);
									informPCAFRLMethod.invoke(p.broadcastHandler(), getter.peer().peerAddress(), storageKey);
								}
							}

						}
					}
				}));
			}

			Thread.sleep(200);
			Field currentExecsField = MapReduceValue.class.getDeclaredField("currentNrOfExecutions");
			currentExecsField.setAccessible(true);

			System.err.println("Here1");
			FutureDone<List<FutureDone<Void>>> future = Futures.whenAll(all).awaitUninterruptibly();
			if (future.isSuccess()) {
				System.err.println("Correct? (" + counts.size() + " >= " + nrOfAcquires + " && " + counts.size() + " < " + nrOfAcquireTries + ")" + (counts.size() >= nrOfAcquires && counts.size() <= nrOfAcquireTries));
				assertEquals(true, counts.size() >= nrOfAcquires && counts.size() <= nrOfAcquireTries);
				for (PeerMapReduce p : p2) {
					Data data = p.taskRPC().storage().get(storageKey);
					if (data != null) {
						MapReduceValue value = ((MapReduceValue) data.object());
						int currentNrOfExecutions = (int) currentExecsField.get(value);
						System.err.println("nrOfAcquires, currentNrOfExecutions? " + nrOfAcquires + ", " + currentNrOfExecutions);
						assertEquals(nrOfAcquires, currentNrOfExecutions);
					}
				}
			} else {
				System.err.println("HERE3 No success on future all");
				fail();
			}

		}
		System.err.println("Here2");

		Thread.sleep(2000);
		for (PeerMapReduce p : p2) {
			p.peer().shutdown().await();
		}

	}

}
