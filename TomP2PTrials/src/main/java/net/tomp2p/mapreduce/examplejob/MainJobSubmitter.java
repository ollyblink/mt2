package net.tomp2p.mapreduce.examplejob;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.mapreduce.FutureTask;
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
		// try {
		boolean shouldBootstrap = true;
		int nrOfShutdownMessagesToAwait = 2;
		int nrOfExecutions = 2;
		// int nrOfFiles = 5;
		PeerConnectionCloseListener.WAITING_TIME = 10000; // Should be less than shutdown time (reps*sleepingTime)
		//
		String filesPath = new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles/";
		//
		int nrOfFiles = localCalculation(filesPath);
		// String filesPath = "/home/ozihler/Desktop/files/testFiles/1";
		Job job = new Job();
		Task startTask = new StartTask(null, NumberUtils.next(), nrOfFiles, nrOfExecutions);
		Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next(), nrOfExecutions);
		Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next(), nrOfExecutions);
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
		// CSG-81: 192.168.1.169
		MapReduceBroadcastHandler broadcastHandler = null;
//		new MapReduceBroadcastHandler();

		Number160 id = new Number160(1);
		PeerMapConfiguration pmc = new PeerMapConfiguration(id);
		pmc.peerNoVerification();
		PeerMap pm = new PeerMap(pmc);
		Peer peer = new PeerBuilder(id).peerMap(pm).ports(4003).broadcastHandler(broadcastHandler).start();
		// String bootstrapperToConnectTo = "192.168.1.172"; //T410
		// String bootstrapperToConnectTo = "192.168.1.143"; //T61 B
		// String bootstrapperToConnectTo = "192.168.1.147"; // ASUS
		String bootstrapperToConnectTo = "192.168.1.169"; // CSG81
		if (shouldBootstrap) {
			// int bootstrapperPortToConnectTo = 4004;
			peer.bootstrap().inetAddress(InetAddress.getByName(bootstrapperToConnectTo))
					// .ports(bootstrapperPortToConnectTo)
					.start().awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureBootstrap>() {

						@Override
						public void operationComplete(FutureBootstrap future) throws Exception {
							if (future.isSuccess()) {
								// System.err.println("successfully bootstrapped to " + bootstrapperToConnectTo + "/"
								// + bootstrapperPortToConnectTo)
								// ;
							} else {
								System.err.println("No success on bootstrapping: fail reason: " + future.failedReason());
							}
						}

					});
		}
		peerMapReduce = new PeerMapReduce(peer, broadcastHandler);
		job.start(input, peerMapReduce);
		// Thread.sleep(10000);
	}
	// finally
	//
	// {
	// // peerMapReduce.peer().shutdown().await();
	// // for (PeerMapReduce p : peers) {
	// // p.peer().shutdown().await();
	// // }
	// }

	// }

	private static int localCalculation(String filesPath) {
		try {
			List<String> pathVisitor = new ArrayList<>();
			FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
			Map<String, Integer> fileResults = new TreeMap<String, Integer>();

			for (String filePath : pathVisitor) {

				try {
					RandomAccessFile aFile = new RandomAccessFile(filePath, "r");
					FileChannel inChannel = aFile.getChannel();
					ByteBuffer buffer = ByteBuffer.allocate(FileSize.MEGA_BYTE.value());
					// int filePartCounter = 0;
					String split = "";
					String actualData = "";
					String remaining = "";
					while (inChannel.read(buffer) > 0) {
						buffer.flip();
						// String all = "";
						// for (int i = 0; i < buffer.limit(); i++) {
						byte[] data = new byte[buffer.limit()];
						buffer.get(data);
						// }
						// System.out.println(all);
						split = new String(data);
						split = remaining += split;

						remaining = "";
						// System.out.println(all);
						// Assure that words are not split in parts by the buffer: only
						// take the split until the last occurrance of " " and then
						// append that to the first again

						if (split.getBytes(Charset.forName("UTF-8")).length >= FileSize.MEGA_BYTE.value()) {
							actualData = split.substring(0, split.lastIndexOf(" ")).trim();
							remaining = split.substring(split.lastIndexOf(" ") + 1, split.length()).trim();
						} else {
							actualData = split.trim();
							remaining = "";
						}
						// System.err.println("Put data: " + actualData + ", remaining data: " + remaining);
						String[] ws = actualData.replaceAll("[\t\n\r]", " ").split(" ");

						for (String word : ws) {
							if (word.trim().length() == 0) {
								continue;
							}
							synchronized (fileResults) {
								Integer ones = fileResults.get(word);
								if (ones == null) {
									ones = 0;
								}
								++ones;
								fileResults.put(word, ones);
							}
						}
						buffer.clear();
						split = "";
						actualData = "";
					}
					inChannel.close();
					aFile.close();
				} catch (Exception e) {
					System.err.println("Exception on reading file at location: " + filePath);
					e.printStackTrace();
				}

			}
			PrintTask.printResults("localOutput", fileResults, -10);
			return pathVisitor.size();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}
}
