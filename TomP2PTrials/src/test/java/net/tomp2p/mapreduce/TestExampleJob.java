package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.junit.Test;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.examplejob.MapTask;
import net.tomp2p.mapreduce.examplejob.PrintTask;
import net.tomp2p.mapreduce.examplejob.ReduceTask;
import net.tomp2p.mapreduce.examplejob.ShutdownTask;
import net.tomp2p.mapreduce.examplejob.StartTask;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class TestExampleJob {
	
	@Test
	public void testJob() throws Exception{
		PeerMapReduce[] peers = null;
		try {
			peers = createAndAttachNodes(100, 4444);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bootstrap(peers);
		perfectRouting(peers);
		int nrOfShutdownMessagesToAwait = 1;

		// String filesPath = new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles/";
		String filesPath = "/home/ozihler/Desktop/files/splitFiles/testfiles";
		Job job = new Job();
		Task startTask = new StartTask(null, NumberUtils.next());
		Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next());
		Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next());
		Task writeTask = new PrintTask(reduceTask.currentId(), NumberUtils.next());
		Task initShutdown = new ShutdownTask(writeTask.currentId(), NumberUtils.next(), nrOfShutdownMessagesToAwait);

		job.addTask(startTask);
		job.addTask(mapTask);
		job.addTask(reduceTask);
		job.addTask(writeTask);
		job.addTask(initShutdown);

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(startTask.currentId()));
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(mapTask.currentId()));
		input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(reduceTask.currentId()));
		input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(writeTask.currentId()));
		input.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), new Data(initShutdown.currentId()));
		input.put(NumberUtils.allSameKey("DATAFILEPATH"), new Data(filesPath));
		input.put(NumberUtils.allSameKey("JOBKEY"), new Data(job.serialize()));

//		DHTWrapper dht = DHTWrapper.create("192.168.1.147", 4003, 4004);
		// DHTWrapper dht = DHTWrapper.create("192.168.1.171", 4004, 4004);
//		MapReduceBroadcastHandler broadcastHandler = new MapReduceBroadcastHandler(dht);
//		dht.broadcastHandler(broadcastHandler);
//		dht.connect();

		// job.mapReduceBroadcastHandler(MapReduceBroadcastHandler.class);
		job.start(input, peers[0].broadcastHandler().dht());
		Thread.sleep(10000);
		for (PeerMapReduce p : peers) {
			p.peer().shutdown().await();
		}
	}

	@Test
	public void testStartTask() throws Exception {
		PeerMapReduce[] peers = null;
		try {
			peers = createAndAttachNodes(100, 4444);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bootstrap(peers);
		perfectRouting(peers);

		String filesPath = (new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles");

		Job job = new Job();
		StartTask startTask = new StartTask(null, NumberUtils.next());
		Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next());
		Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next());
		Task writeTask = new PrintTask(reduceTask.currentId(), NumberUtils.next());
		Task initShutdown = new ShutdownTask(writeTask.currentId(), NumberUtils.next(), 2);

		job.addTask(startTask);
		job.addTask(mapTask);
		job.addTask(reduceTask);
		job.addTask(writeTask);
		job.addTask(initShutdown);

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(startTask.currentId()));
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(mapTask.currentId()));
		input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(reduceTask.currentId()));
		input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(writeTask.currentId()));
		input.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), new Data(initShutdown.currentId()));
		input.put(NumberUtils.allSameKey("DATAFILEPATH"), new Data(filesPath));
		input.put(NumberUtils.allSameKey("JOBKEY"), new Data(job.serialize()));
		startTask.broadcastReceiver(input, peers[0].broadcastHandler().dht());

		Thread.sleep(1000);
		FutureTask get = peers[10].get(Number160.createHash(filesPath + "/testfile.txt"), Number160.createHash(peers[0].peer().peerID() + "_" + 0), input).start();
		get.addListener(new BaseFutureAdapter<FutureTask>() {

			@Override
			public void operationComplete(FutureTask future) throws Exception {
				if (future.isSuccess()) {
					String content = (String) future.data().object();
					System.err.println("Content : [" + content + "]");
				}else{
					System.err.println("No success on getting data for "+filesPath + "/testfile.txt");
				}
			}

		}).awaitUninterruptibly();
		get = peers[18].get(Number160.createHash(filesPath + "/testfile2.txt"), Number160.createHash(peers[0].peer().peerID() + "_" + 0), input).start();
		get.addListener(new BaseFutureAdapter<FutureTask>() {

			@Override
			public void operationComplete(FutureTask future) throws Exception {
				if (future.isSuccess()) {
					String content = (String) future.data().object();
					System.err.println("Content : [" + content + "]");
				}else{
					System.err.println("No success on getting data for "+filesPath + "/testfile2.txt");
				}
			}

		}).awaitUninterruptibly();
		get = peers[85].get(Number160.createHash(filesPath + "/testfile3.txt"), Number160.createHash(peers[0].peer().peerID() + "_" + 0), input).start();
		get.addListener(new BaseFutureAdapter<FutureTask>() {

			@Override
			public void operationComplete(FutureTask future) throws Exception {
				if (future.isSuccess()) {
					String content = (String) future.data().object();
					System.err.println("Content : [" + content + "]");
				}else{
					System.err.println("No success on getting data for "+filesPath + "/testfile3.txt");
				}
			}

		}).awaitUninterruptibly();
		Thread.sleep(5000);
		for (PeerMapReduce p : peers) {
			p.peer().shutdown().await();
		}
	}

	@Test
	public void testMapTask() throws Exception {
		MapTask maptask = new MapTask(NumberUtils.allSameKey("INITTASKID"), NumberUtils.allSameKey("MAPTASKID"));
		PeerMapReduce[] peers = null;
		try {
			peers = createAndAttachNodes(100, 4444);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bootstrap(peers);
		perfectRouting(peers);

		Number160 fileLocationKey = Number160.createHash("FILE1");
		Number160 domainKey = Number160.createHash(peers[0].peer().peerID() + "_" + System.currentTimeMillis());
		peers[0].put(fileLocationKey, domainKey, "hello world hello world hello world", 3).start().awaitUninterruptibly();

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.STORAGE_KEY, new Data(new Number640(fileLocationKey, domainKey, Number160.ZERO, Number160.ZERO)));
		maptask.broadcastReceiver(input, peers[0].broadcastHandler().dht());

		Thread.sleep(1000);
		FutureTask get = peers[10].get(fileLocationKey, Number160.createHash(peers[0].peer().peerID() + "_" + (0)), input).start();
		get.addListener(new BaseFutureAdapter<FutureTask>() {

			@Override
			public void operationComplete(FutureTask future) throws Exception {
				if (future.isSuccess()) {
					Map<String, Integer> fileWords = (Map<String, Integer>) future.data().object();
					System.out.println(fileWords);
					assertEquals(2, fileWords.keySet().size());
					assertEquals(true, fileWords.containsKey("hello"));
					assertEquals(true, fileWords.containsKey("world"));
					assertEquals(new Integer(3), fileWords.get("hello"));
					assertEquals(new Integer(3), fileWords.get("world"));
				}
			}

		}).awaitUninterruptibly();
		// Thread.sleep(5000);
		for (PeerMapReduce p : peers) {
			p.peer().shutdown().await();
		}
	}

	@Test
	public void testReduceTask() throws Exception {
		ReduceTask reduceTask = new ReduceTask(NumberUtils.allSameKey("MAPTASKID"), NumberUtils.allSameKey("REDUCETASKID"));
		PeerMapReduce[] peers = null;
		try {
			peers = createAndAttachNodes(100, 4444);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bootstrap(peers);
		perfectRouting(peers);

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("NUMBEROFFILES"), new Data(4));
		for (int i = 0; i < 4; ++i) {
			Number160 fileLocationKey = Number160.createHash("FILE" + i);
			Number160 domainKey = Number160.createHash(peers[0].peer().peerID() + "_" + i);
			Map<String, Integer> values = new HashMap<>();
			if (i % 2 == 1) {
				values.put("hello", 3);
			}
			values.put("world", 4);
			if (i % 2 == 0) {
				values.put("test", 5);
			}
			System.err.println("Values to put: " + values);

			peers[0].put(fileLocationKey, domainKey, values, 1).start().awaitUninterruptibly();

			input.put(NumberUtils.STORAGE_KEY, new Data(new Number640(fileLocationKey, domainKey, Number160.ZERO, Number160.ZERO)));
			reduceTask.broadcastReceiver(input, peers[0].broadcastHandler().dht());

		}

		Thread.sleep(1000);
		FutureTask get = peers[10].get(Number160.createHash("FINALRESULT"), Number160.createHash(peers[0].peer().peerID() + "_" + (0)), input).start();
		get.addListener(new BaseFutureAdapter<FutureTask>() {

			@Override
			public void operationComplete(FutureTask future) throws Exception {
				if (future.isSuccess()) {
					Map<String, Integer> fileWords = (Map<String, Integer>) future.data().object();
					System.err.println(fileWords);
					assertEquals(3, fileWords.keySet().size());
					assertEquals(true, fileWords.containsKey("hello"));
					assertEquals(true, fileWords.containsKey("world"));
					assertEquals(true, fileWords.containsKey("test"));
					assertEquals(new Integer(6), fileWords.get("hello"));
					assertEquals(new Integer(16), fileWords.get("world"));
					assertEquals(new Integer(10), fileWords.get("test"));
				}
			}

		}).awaitUninterruptibly();
		// Thread.sleep(5000);
		for (PeerMapReduce p : peers) {
			p.peer().shutdown().await();
		}
	}

	@Test
	public void testPrintTask() throws Exception {
		PrintTask maptask = new PrintTask(NumberUtils.allSameKey("REDUCETASKID"), NumberUtils.allSameKey("WRITETASKID"));
		PeerMapReduce[] peers = null;
		try {
			peers = createAndAttachNodes(100, 4444);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bootstrap(peers);
		perfectRouting(peers);

		Number160 resKey = Number160.createHash("FINALRESULT");
		Number160 domainKey = Number160.createHash(peers[0].peer().peerID() + "_" + System.currentTimeMillis());

		Map<String, Integer> values = new HashMap<>();

		values.put("hello", 1);
		values.put("world", 6);
		values.put("test", 2);
		values.put("this", 8);

		peers[0].put(resKey, domainKey, values, 3).start().awaitUninterruptibly();

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), new Data(NumberUtils.next()));
		input.put(NumberUtils.STORAGE_KEY, new Data(new Number640(resKey, domainKey, Number160.ZERO, Number160.ZERO)));
		maptask.broadcastReceiver(input, peers[0].broadcastHandler().dht());

		Thread.sleep(1000);

		for (PeerMapReduce p : peers) {
			p.peer().shutdown().await();
		}
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
			DHTWrapper mockDHT = DHTWrapper.create("", 0, 0);
			if (i == 0) {
				peers[0] = new PeerMapReduce(new PeerBuilder(new Number160(RND)).ports(port).start());
			} else {
				peers[i] = new PeerMapReduce(new PeerBuilder(new Number160(RND)).masterPeer(peers[0].peer()).start());
			}
		}
		return peers;
	}
}
