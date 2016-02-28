package net.tomp2p.mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import mapreduce.storage.DHTConnectionProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.utils.FileSplitter;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class Main {

	public static class StartTask extends Task {

		public StartTask(Number640 previousId, Number640 currentId) {
			super(previousId, currentId);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -5879889214195971852L;

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht) throws Exception {

			final List<FuturePut> futurePuts = Collections.synchronizedList(new ArrayList<>());
			// Put data
			String filesPath = (String) input.get(NumberUtils.allSameKey("DATAFILEPATH")).object();
			Number160 jobKey = Number160.createHash("JOBKEY");
			futurePuts.add(dht.put(jobKey, input.get(NumberUtils.allSameKey("JOBKEY"))));

			List<String> pathVisitor = Collections.synchronizedList(new ArrayList<>());
			FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
			System.out.println("All files: " + pathVisitor);

			List<Number160> fileKeys = Collections.synchronizedList(new ArrayList<>());

			for (String filePath : pathVisitor) {
				Map<Number160, FuturePut> tmp = FileSplitter.readFile(filePath, dht, FileSize.MEGA_BYTE.value(), "UTF-8");
				fileKeys.addAll(tmp.keySet());
				futurePuts.addAll(tmp.values());
			}
			System.out.println("File keys size:" + fileKeys.size());
			// Put job

			FutureDone<List<FuturePut>> initial = Futures.whenAllSuccess(futurePuts) // TODO that is not so nice...
					.addListener(new BaseFutureAdapter<BaseFuture>() {

						@Override
						public void operationComplete(BaseFuture future) throws Exception {
							if (future.isSuccess()) {
								NavigableMap<Number640, Data> newInput = new TreeMap<>();
								keepTaskIDs(input, newInput);
								newInput.put(NumberUtils.allSameKey("CURRENTTASK"), input.get(NumberUtils.allSameKey("INPUTTASKID")));
								newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get(NumberUtils.allSameKey("MAPTASKID")));
								newInput.put(NumberUtils.allSameKey("FILEKEYS"), new Data(fileKeys));
								newInput.put(NumberUtils.allSameKey("JOBKEY"), new Data(jobKey));
								newInput.put(NumberUtils.allSameKey("SENDERID"), new Data(dht.peerDHT().peerID()));
								dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
							} else {
								// Do nothing
							}
						}

					});
			
			Futures.whenAllSuccess(initial);
		}

	}

	public static class MapTask extends Task {

		public MapTask(Number640 previousId, Number640 currentId) {
			super(previousId, currentId);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 7150229043957182808L;

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht) throws Exception {

			List<Number160> allDataKeys = (List<Number160>) input.get(NumberUtils.allSameKey("FILEKEYS")).object();
			List<FutureGet> getData = Collections.synchronizedList(new ArrayList<>());
			List<FuturePut> putWords = Collections.synchronizedList(new ArrayList<>());
			Set<String> words = Collections.synchronizedSet(new HashSet<>());
			Number160 domainKey = Number160.createHash(dht.peerDHT().peerID() + "_" + System.currentTimeMillis());

			for (Number160 dataKey : allDataKeys) {
				getData.add(dht.get(dataKey).addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							String text = ((String) future.data().object()).replaceAll("[\t\n\r]", " ");
							System.out.println("Text: " + text);
							String[] ws = text.split(" ");
							for (String word : ws) {
								if (word.trim().length() == 0) {
									continue;
								}
								words.add(word);
								putWords.add(dht.addAsList(Number160.createHash(word), 1, domainKey));
								// if(counter++%1000 == 0){
								System.out.println("MAP: ADD(" + word + ", " + 1 + ")");
								// }
							}
						} else {
							// Do nothing
						}
					}

				}));
			}

			Futures.whenAllSuccess(getData).addListener(new BaseFutureAdapter<BaseFuture>() {

				@Override
				public void operationComplete(BaseFuture future) throws Exception {
					if (future.isSuccess()) {
						Futures.whenAllSuccess(putWords).addListener(new BaseFutureAdapter<BaseFuture>() {

							@Override
							public void operationComplete(BaseFuture future) throws Exception {
								if (future.isSuccess()) {
									NavigableMap<Number640, Data> newInput = new TreeMap<>();
									keepTaskIDs(input, newInput);
									newInput.put(NumberUtils.allSameKey("CURRENTTASK"), input.get(NumberUtils.allSameKey("MAPTASKID")));
									newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get(NumberUtils.allSameKey("REDUCETASKID")));
									newInput.put(NumberUtils.allSameKey("WORDS"), new Data(words));
									newInput.put(NumberUtils.allSameKey("DOMAINKEY"), new Data(domainKey));
									newInput.put(NumberUtils.allSameKey("SENDERID"), new Data(dht.peerDHT().peerID()));
									dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
								}
							}
						});
					}
				}

			});

		}

	}

	public static class ReduceTask extends Task {

		public ReduceTask(Number640 previousId, Number640 currentId) {
			super(previousId, currentId);
		}

		/**
		* 
		*/
		private static final long serialVersionUID = -5662749658082184304L;

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht) throws Exception {

			Set<String> words = (Set<String>) input.get(NumberUtils.allSameKey("WORDS")).object();
			Number160 receivedDomainKey = (Number160) input.get(NumberUtils.allSameKey("DOMAINKEY")).object();
			List<FutureGet> getData = Collections.synchronizedList(new ArrayList<>());
			List<FuturePut> putWords = Collections.synchronizedList(new ArrayList<>());

			Number160 domainKey = Number160.createHash(dht.peerDHT().peerID() + "_" + System.currentTimeMillis());

			Set<String> words2 = Collections.synchronizedSet(new HashSet<>());
			for (String word : words) {
				Number160 wordKeyHash = Number160.createHash(word);
				getData.add(dht.getAll(wordKeyHash, receivedDomainKey).addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							words2.add(word);
							int sum = future.dataMap().keySet().size();
							System.out.println("REDUCE: ADD(" + word + ", " + sum + ")");
							putWords.add(dht.put(wordKeyHash, sum, domainKey));
						} else {
							// Do nothing
						}
					}

				}));
			}
			Futures.whenAllSuccess(getData).addListener(new BaseFutureAdapter<BaseFuture>() {

				@Override
				public void operationComplete(BaseFuture future) throws Exception {
					if (future.isSuccess()) {
						Futures.whenAllSuccess(putWords).addListener(new BaseFutureAdapter<BaseFuture>() {

							@Override
							public void operationComplete(BaseFuture future) throws Exception {
								if (future.isSuccess()) {
									NavigableMap<Number640, Data> newInput = new TreeMap<>();
									keepTaskIDs(input, newInput);
									newInput.put(NumberUtils.allSameKey("CURRENTTASK"), input.get(NumberUtils.allSameKey("REDUCETASKID")));
									newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get(NumberUtils.allSameKey("WRITETASKID")));
									newInput.put(NumberUtils.allSameKey("WORDS"), new Data(words2));
									newInput.put(NumberUtils.allSameKey("DOMAIN"), new Data(domainKey));
									newInput.put(NumberUtils.allSameKey("SENDERID"), new Data(dht.peerDHT().peerID()));
									dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
								}
							}
						});
					}
				}
			});
		}

	}

	public static class PrintTask extends Task {
		public PrintTask(Number640 previousId, Number640 currentId) {
			super(previousId, currentId);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -8206142810699508919L;

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht) throws Exception {

			Set<String> words = (Set<String>) input.get(NumberUtils.allSameKey("WORDS")).object();
			Number160 receivedDomainKey = (Number160) input.get(NumberUtils.allSameKey("DOMAIN")).object();
			List<FutureGet> getData = Collections.synchronizedList(new ArrayList<>());

			final Map<String, Integer> results = Collections.synchronizedMap(new HashMap<>());
			for (String word : words) {
				getData.add(dht.get(Number160.createHash(word), receivedDomainKey).addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							results.put(word, (Integer) future.data().object());
						} else {
							// Do nothing
						}
					}

				}));
			}
			Futures.whenAllSuccess(getData).addListener(new BaseFutureAdapter<BaseFuture>() {

				@Override
				public void operationComplete(BaseFuture future) throws Exception {
					if (future.isSuccess()) {
						List<String> wordList = new ArrayList<>(results.keySet());
						Collections.sort(wordList);
						System.out.println("==========WORDCOUNT RESULTS OF PEER WITH ID: " + dht.peerDHT().peerID().intValue() + "==========");
						System.out.println("=====================================");
						for (String word : wordList) {
							System.out.println(word + " " + results.get(word));
						}
						System.out.println("=====================================");
						NavigableMap<Number640, Data> newInput = new TreeMap<>();
						keepTaskIDs(input, newInput);
//						newInput.put(NumberUtils.allSameKey("CURRENTTASK"), input.get(NumberUtils.allSameKey("WRITETASKID")));
//						newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
//						newInput.put(NumberUtils.allSameKey("SENDERID"), new Data(dht.peerDHT().peerID()));
//						dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);

					}
				}
			});
		}

	}

	public static class ShutdownTask extends Task {
		public ShutdownTask(Number640 previousId, Number640 currentId) {
			super(previousId, currentId);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -5543401293112052880L;

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht) throws Exception {
			dht.shutdown();
			System.out.println("Successfully shutdown dht");
		}
	}

	public static void main(String[] args) throws Exception {

		String filesPath = new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles/";
		Job job = new Job();
		Task startTask = new StartTask(null, NumberUtils.next());
		Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next());
		Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next());
		Task writeTask = new PrintTask(reduceTask.currentId(), NumberUtils.next());
//		Task initShutdown = new ShutdownTask(writeTask.currentId(), NumberUtils.next());

		job.addTask(startTask);
		job.addTask(mapTask);
		job.addTask(reduceTask);
		job.addTask(writeTask);
//		job.addTask(initShutdown);

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(startTask.currentId()));
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(mapTask.currentId()));
		input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(reduceTask.currentId()));
		input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(writeTask.currentId()));
//		input.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), new Data(initShutdown.currentId()));
		input.put(NumberUtils.allSameKey("DATAFILEPATH"), new Data(filesPath));
		input.put(NumberUtils.allSameKey("JOBKEY"), new Data(job.serialize()));

		DHTConnectionProvider dht = DHTConnectionProvider.create("192.168.43.65", 4000, 4001);
		MapReduceBroadcastHandler broadcastHandler = new MapReduceBroadcastHandler(dht);
		dht.broadcastHandler(broadcastHandler);
		dht.connect();

		job.start(input, dht); 

	}

	private static void keepTaskIDs(NavigableMap<Number640, Data> input, NavigableMap<Number640, Data> newInput) {
		newInput.put(NumberUtils.allSameKey("INPUTTASKID"), input.get(NumberUtils.allSameKey("INPUTTASKID")));
		newInput.put(NumberUtils.allSameKey("MAPTASKID"), input.get(NumberUtils.allSameKey("MAPTASKID")));
		newInput.put(NumberUtils.allSameKey("REDUCETASKID"), input.get(NumberUtils.allSameKey("REDUCETASKID")));
		newInput.put(NumberUtils.allSameKey("WRITETASKID"), input.get(NumberUtils.allSameKey("WRITETASKID")));
//		newInput.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
	}
}
