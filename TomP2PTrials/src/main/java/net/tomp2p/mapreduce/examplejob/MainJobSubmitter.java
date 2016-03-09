package net.tomp2p.mapreduce.examplejob;

import java.io.File;
import java.net.InetAddress;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.mapreduce.Job;
import net.tomp2p.mapreduce.MapReduceBroadcastHandler;
import net.tomp2p.mapreduce.PeerConnectionCloseListener;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.storage.Data;

public class MainJobSubmitter {
	public static void main(String[] args) throws Exception {
		PeerMapReduce peerMapReduce = null;

		// PeerMapReduce[] peers = null;
		// try {
		// peers = createAndAttachNodes(1, 4444);
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// bootstrap(peers);
		// perfectRouting(peers);
		try {
			int nrOfShutdownMessagesToAwait = 1;
			PeerConnectionCloseListener.WAITING_TIME = 10000; // Should be less than shutdown time (reps*sleepingTime)

//			String filesPath = new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles/";
			 String filesPath = "/home/ozihler/Desktop/files/splitFiles/3";
			Job job = new Job();
			Task startTask = new StartTask(null, NumberUtils.next(), 100, 1);
			Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next());
			Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next());
			Task writeTask = new PrintTask(reduceTask.currentId(), NumberUtils.next());
			Task initShutdown = new ShutdownTask(mapTask.currentId(), NumberUtils.next(), nrOfShutdownMessagesToAwait, 13, 1000);

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
			input.put(NumberUtils.JOB_KEY, new Data(job.serialize()));
			// T410: 192.168.1.172
			// ASUS: 192.168.1.147
			MapReduceBroadcastHandler broadcastHandler = new MapReduceBroadcastHandler();

			Number160 id = new Number160(1);
			PeerMapConfiguration pmc = new PeerMapConfiguration(id);
			pmc.peerNoVerification();
			PeerMap pm = new PeerMap(pmc);
			Peer peer = new PeerBuilder(id).peerMap(pm).ports(4003).broadcastHandler(broadcastHandler).start();
			// String bootstrapperToConnectTo = "192.168.1.172"; //T410

			String bootstrapperToConnectTo = "192.168.1.147"; // ASUS
//			int bootstrapperPortToConnectTo = 4004;
//			peer.bootstrap().inetAddress(InetAddress.getByName(bootstrapperToConnectTo)).ports(bootstrapperPortToConnectTo).start().awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureBootstrap>() {
//
//				@Override
//				public void operationComplete(FutureBootstrap future) throws Exception {
//					if (future.isSuccess()) {
//						System.err.println("successfully bootstrapped to " + bootstrapperToConnectTo + "/" + bootstrapperPortToConnectTo);
//					} else {
//						System.err.println("No success on bootstrapping: fail reason: " + future.failedReason());
//					}
//				}
//
//			});
			peerMapReduce = new PeerMapReduce(peer, broadcastHandler);
			job.start(input, peerMapReduce);
			// Thread.sleep(10000);
		} finally {
			// peerMapReduce.peer().shutdown().await();
			// for (PeerMapReduce p : peers) {
			// p.peer().shutdown().await();
			// }
		}

	}
}
