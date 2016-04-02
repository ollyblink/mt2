package net.tomp2p.mapreduce.examplejob;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.mapreduce.IMapReduceBroadcastReceiver;
import net.tomp2p.mapreduce.Job;
import net.tomp2p.mapreduce.PeerConnectionCloseListener;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.storage.Data;

public class MainJobSubmitter {
	private static final int waitingTime = 7000;
	private static final long nrOfNodes = 5;

	private static NavigableMap<Number640, Data> getJob(int nrOfShutdownMessagesToAwait, int nrOfExecutions, String filesPath, int nrOfFiles, Job job) throws IOException {
		Task startTask = new StartTask(null, NumberUtils.next(), nrOfExecutions);
		Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next(), nrOfExecutions);
		Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next(), nrOfExecutions);
		Task writeTask = new PrintTask(reduceTask.currentId(), NumberUtils.next());
		Task initShutdown = new ShutdownTask(writeTask.currentId(), NumberUtils.next(), nrOfShutdownMessagesToAwait, 10, 1000, nrOfNodes);

		job.addTask(startTask);
		job.addTask(mapTask);
		job.addTask(reduceTask);
		job.addTask(writeTask);
		job.addTask(initShutdown);

		// Add receiver to handle BC messages (job specific handler, defined by user)
		IMapReduceBroadcastReceiver receiver = new ExampleJobBroadcastReceiver(job.id());
		job.addBroadcastReceiver(receiver);

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(startTask.currentId()));
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(mapTask.currentId()));
		input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(reduceTask.currentId()));
		input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(writeTask.currentId()));
		input.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), new Data(initShutdown.currentId()));
		input.put(NumberUtils.allSameKey("DATAFILEPATH"), new Data(filesPath));
		input.put(NumberUtils.allSameKey("NUMBEROFFILES"), new Data(nrOfFiles));

		return input;
	}

	public static void main(String[] args) throws Exception {
		//
		// boolean shouldBootstrap = false;
		// int nrOfShutdownMessagesToAwait = 1;
		// int nrOfExecutions = 1;
		boolean shouldBootstrap = true;
		int nrOfShutdownMessagesToAwait = 2;
		int nrOfExecutions = 2;
		ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_TCP_IDLE_MILLIS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP = Integer.MAX_VALUE;
		// ConnectionBean.DEFAULT_UDP_IDLE_MILLIS = 10000;

		PeerConnectionCloseListener.WAITING_TIME = Integer.MAX_VALUE; // Should be less than shutdown time (reps*sleepingTime)

		int bootstrapperPortToConnectTo = 4004;
		// String bootstrapperToConnectTo = "192.168.1.172"; //T410
		// String bootstrapperToConnectTo = "192.168.1.16"; //T410 ANDROID
		// String bootstrapperToConnectTo = "192.168.43.144"; //T61ANDROID
		// // String bootstrapperToConnectTo = "130.60.156.102"; //T61 B
		// String bootstrapperToConnectTo = "192.168.0.17"; // T61 B
		// String bootstrapperToConnectTo = "192.168.0.15"; // ASUS
		String bootstrapperToConnectTo = "130.60.156.102"; // ASUS UNI ETH

		// String bootstrapperToConnectTo = "192.168.1.143"; //T61 B
		// String bootstrapperToConnectTo = "192.168.1.147"; // ASUS
		// String bootstrapperToConnectTo = "192.168.43.59"; // T61c ANDROID S6
		// String bootstrapperToConnectTo = "192.168.1.147"; // CSG81
		Number160 id = new Number160(1);

		PeerMapConfiguration pmc = new PeerMapConfiguration(id);
		pmc.peerNoVerification();
		PeerMap pm = new PeerMap(pmc);
		PeerBuilder peerBuilder = new PeerBuilder(id).peerMap(pm).ports(bootstrapperPortToConnectTo);

		PeerMapReduce peerMapReduce = new PeerMapReduce(peerBuilder, waitingTime);
		if (shouldBootstrap) {
			// int bootstrapperPortToConnectTo = 4004;
			peerMapReduce.peer().bootstrap().ports(bootstrapperPortToConnectTo).inetAddress(InetAddress.getByName(bootstrapperToConnectTo))
					// .ports(bootstrapperPortToConnectTo)
					.start().awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureBootstrap>() {

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
		// new Thread(new Runnable() {
		//
		// @Override
		// public void run() {
		// Sniffer.main(null);
		// }
		// }).start();
		final PeerMapReduce pmr = peerMapReduce;
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					System.err.println("Sleeping for 20secs before executing job");

					Thread.sleep(1);

					// String filesPath = new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles/";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/512kb/1MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1MB/1MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/512kb/2MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1MB/2MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1File/2MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/512kb/4MB";
					 String filesPath = "/home/ozihler/Desktop/files/evaluation/1MB/4MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1File/4MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/512kb/8MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1MB/8MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1File/8MB";
//					String filesPath = "/home/ozihler/Desktop/files/evaluation/512kb/12MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1MB/12MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1File/12MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/512kb/16MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1MB/16MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1File/16MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/512kb/20MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1MB/20MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/1File/20MB";
					// String filesPath = "/home/ozihler/Desktop/files/evaluation/8MB/get";
					//
					int nrOfFiles = localCalculation(filesPath);
					// nrOfFiles = ;
					// String filesPath = "/home/ozihler/Desktop/files/testFiles/1";

					System.err.println("MainJobSubmitter: nrOfShutdownMessagesToAwait[" + nrOfShutdownMessagesToAwait + "], nrOfExecutions[" + nrOfExecutions + "], ConnectionBean.DEFAULT_TCP_IDLE_MILLIS[" + ConnectionBean.DEFAULT_TCP_IDLE_MILLIS + "], PeerConnectionCloseListener.WAITING_TIME ["
							+ PeerConnectionCloseListener.WAITING_TIME + "], filesPath[" + filesPath + "], nrOfFiles [" + nrOfFiles + "]");
					System.err.println("MainJobSubmitter: START JOB");

					Job job = new Job(new Number640(new Random()));
					NavigableMap<Number640, Data> input = getJob(nrOfShutdownMessagesToAwait, nrOfExecutions, filesPath, nrOfFiles, job);
					job.start(input, pmr);

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}).start();

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
					ByteBuffer buffer = ByteBuffer.allocate(FileSize.SIXTY_FOUR_MEGA_BYTES.value());
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
						// System.out.println(all);d
						// Assure that words are not split in parts by the buffer: only
						// take the split until the last occurrance of " " and then
						// append that to the first again

						if (split.getBytes(Charset.forName("UTF-8")).length >= FileSize.SIXTY_FOUR_MEGA_BYTES.value()) {
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
			System.err.println("Nr of words to expect: " + fileResults.keySet().size());
			PrintTask.printResults("localOutput", fileResults, -10);
			return pathVisitor.size();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}
}
