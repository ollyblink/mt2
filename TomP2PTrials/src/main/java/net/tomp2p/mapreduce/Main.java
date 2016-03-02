package net.tomp2p.mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTWrapper;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.utils.FileSplitter;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class Main {

	public static class StartTask extends Task {
		private static Logger logger = LoggerFactory.getLogger(StartTask.class);

		public StartTask(Number640 previousId, Number640 currentId) {
			super(previousId, currentId);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -5879889214195971852L;

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTWrapper dht) throws Exception {

			Number160 jobKey = Number160.createHash("JOBKEY");
			Number160 domainKey = Number160.createHash(dht.peerDHT().peerID() + "_" + System.currentTimeMillis());

			Number640 jobStorageKey = new Number640(jobKey, domainKey, null, null);
			Data jobToPut = input.get(NumberUtils.allSameKey("JOBKEY"));
			dht.put(jobStorageKey.locationKey(), jobToPut, jobStorageKey.domainKey()).addListener(new BaseFutureAdapter<FuturePut>() {

				@Override
				public void operationComplete(FuturePut future) throws Exception {
					if (future.isSuccess()) {
						logger.info("Sucess on put(Job) with key " + jobKey + ", continue to put data for job");
						// =====END NEW BC DATA===========================================================
						Map<Number640, Data> tmpNewInput = Collections.synchronizedMap(new TreeMap<>()); // Only used to avoid adding it in each future listener...
						keepTaskIDs(input, tmpNewInput);
						tmpNewInput.put(NumberUtils.allSameKey("CURRENTTASK"), input.get(NumberUtils.allSameKey("INPUTTASKID")));
						tmpNewInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get(NumberUtils.allSameKey("MAPTASKID")));
						tmpNewInput.put(NumberUtils.allSameKey("JOBKEY"), new Data(jobKey));

						SimpleBroadcastReceiver r = new SimpleBroadcastReceiver();
						Map<String, byte[]> bcClassFiles = SerializeUtils.serializeClassFile(SimpleBroadcastReceiver.class);
						String bcClassName = SimpleBroadcastReceiver.class.getName();
						byte[] bcObject = SerializeUtils.serializeJavaObject(r);
						TransferObject t = new TransferObject(bcObject, bcClassFiles, bcClassName);
						List<TransferObject> broadcastReceivers = new ArrayList<>();
						broadcastReceivers.add(t);

						tmpNewInput.put(NumberUtils.allSameKey("RECEIVERS"), new Data(broadcastReceivers).to);
						tmpNewInput.put(NumberUtils.allSameKey("SENDERID"), new Data(dht.peerDHT().peerID())); // Don't need that, can simply use message.sender() for that? is peerId though
						// =====END NEW BC DATA===========================================================
						// ============GET ALL THE FILES ==========
						String filesPath = (String) input.get(NumberUtils.allSameKey("DATAFILEPATH")).object();
						List<String> pathVisitor = Collections.synchronizedList(new ArrayList<>());
						FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
						// ===== FINISHED GET ALL THE FILES ========
						
						final List<FuturePut> futurePuts = Collections.synchronizedList(new ArrayList<>());
						for (String filePath : pathVisitor) {
							Map<Number160, FuturePut> tmp = FileSplitter.splitWithWordsAndWrite(filePath, dht, FileSize.MEGA_BYTE.value(), "UTF-8");
							for (Number160 fileKey : tmp.keySet()) {
								tmp.get(fileKey).addListener(new BaseFutureAdapter<FuturePut>() {

									@Override
									public void operationComplete(FuturePut future) throws Exception {
										if (future.isSuccess()) {
											NavigableMap<Number640, Data> newInput = new TreeMap<>();
											synchronized (tmpNewInput) {
												newInput.putAll(tmpNewInput);
											}
											Number640 storageKey = new Number640(fileKey, domainKey, null, null); // Actual key
											newInput.put(NumberUtils.allSameKey("STORAGE_KEY"), new Data(storageKey));
											// Here: instead of futures when all, already send out broadcast
											dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
											logger.info("success on put(fileKey, actualValues) and broadcast for key: " + fileKey);
										} else {
											logger.info("No success on put(fileKey, actualValues) for key " + fileKey);
										}
									}

								});
							}
							// fileKeys.addAll(tmp.keySet());
							futurePuts.addAll(tmp.values());
						}
						// logger.info("File keys size:" + fileKeys.size());
						// Just for information! Has no actual value only for the user to be informed if something went wrong, although this will already be shown in each failed BaseFutureAdapter<FuturePut> above
						FutureDone<List<FuturePut>> initial = Futures.whenAllSuccess(futurePuts).addListener(new BaseFutureAdapter<BaseFuture>() {

							@Override
							public void operationComplete(BaseFuture future) throws Exception {
								if (future.isSuccess()) {
									logger.info("Successfully put and broadcasted all file splits");
								} else {
									logger.info("No success on putting all files splits/broadcasting all keys! Fail reason: " + future.failedReason());
								}
							}
						});
					} else {
						logger.info("No sucess on put(Job): Fail reason: " + future.failedReason());
					}
				}
			});

			// Futures.whenAllSuccess(initial);
		}

	}

	public static class MapTask extends Task {
		private static Logger logger = LoggerFactory.getLogger(MapTask.class);

		public MapTask(Number640 previousId, Number640 currentId) {
			super(previousId, currentId);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 7150229043957182808L;

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTWrapper dht) throws Exception {
			logger.info("Executing Map Task");
			List<Number160> allDataKeys = (List<Number160>) input.get(NumberUtils.allSameKey("FILEKEYS")).object();
			List<FutureGet> getData = Collections.synchronizedList(new ArrayList<>());
			List<FuturePut> putWords = Collections.synchronizedList(new ArrayList<>());
			Set<String> words = Collections.synchronizedSet(new HashSet<>());
			Number160 domainKey = Number160.createHash(dht.peerDHT().peerID() + "_" + System.currentTimeMillis());

			Map<String, Integer> forFile = Collections.synchronizedMap(new HashMap<String, Integer>());
			for (Number160 dataKey : allDataKeys) {
				getData.add(dht.get(dataKey/* , input.put(NumberUtils.allSameKey("DATAKEY"), dataKey) --> this input is used to resubmit the broadcast if needed */).addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							String text = ((String) future.data().object()).replaceAll("[\t\n\r]", " ");
							// logger.info("Text: " + text);
							String[] ws = text.split(" ");
							int counter = 0;
							for (String word : ws) {
								if (word.trim().length() == 0) {
									continue;
								}
								synchronized (forFile) {
									Integer ones = forFile.get(word);
									if (ones == null) {
										ones = 0;
									}
									++ones;
									forFile.put(word, ones);
								}
								// words.add(word);
								if (counter++ % 10000000 == 0) {
									byte[] tmpSize = SerializeUtils.serializeJavaObject(forFile);
									if (tmpSize.length >= FileSize.EIGHT_KILO_BYTES.value()) {
										synchronized (forFile) {
											for (String w : forFile.keySet()) {
												logger.info("add(" + w + ", " + forFile.get(w) + ").domain(" + domainKey + ")");
												putWords.add(dht.addAsList(Number160.createHash(w), forFile.get(w), domainKey));
											}
											words.addAll(forFile.keySet());
											forFile.clear();
										}
									}
								}
							}
							logger.info("After: nr of words " + words.size());
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
						if (forFile.size() > 0) {
							synchronized (forFile) {
								for (String w : forFile.keySet()) {
									logger.info("add(" + w + ", " + forFile.get(w) + ").domain(" + domainKey + ")");
									putWords.add(dht.addAsList(Number160.createHash(w), forFile.get(w), domainKey));
								}
								words.addAll(forFile.keySet());
								forFile.clear();
							}
						}
						logger.info("Final After: nr of words " + words.size());
						logger.info("getData success, putWords: " + putWords.size());
						Futures.whenAllSuccess(putWords).addListener(new BaseFutureAdapter<BaseFuture>() {

							@Override
							public void operationComplete(BaseFuture future) throws Exception {
								if (future.isSuccess()) {
									NavigableMap<Number640, Data> newInput = new TreeMap<>();
									logger.info("putWords future.isSuccess");
									keepTaskIDs(input, newInput);
									newInput.put(NumberUtils.allSameKey("CURRENTTASK"), input.get(NumberUtils.allSameKey("MAPTASKID")));
									newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get(NumberUtils.allSameKey("REDUCETASKID")));
									newInput.put(NumberUtils.allSameKey("WORDS"), new Data(words));
									newInput.put(NumberUtils.allSameKey("DOMAINKEY"), new Data(domainKey));
									newInput.put(NumberUtils.allSameKey("SENDERID"), new Data(dht.peerDHT().peerID()));

									dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
								} else {
									logger.info("Future broadcast for Map task failed, " + future.failedReason());
								}
							}
						});
					}
				}
			});
		}

	}

	public static class ReduceTask extends Task {
		private static Logger logger = LoggerFactory.getLogger(ReduceTask.class);

		public ReduceTask(Number640 previousId, Number640 currentId) {
			super(previousId, currentId);
		}

		/**
		* 
		*/
		private static final long serialVersionUID = -5662749658082184304L;

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTWrapper dht) throws Exception {
			logger.info("Executing Reduce Task");
			Set<String> words = (Set<String>) input.get(NumberUtils.allSameKey("WORDS")).object();
			Number160 receivedDomainKey = (Number160) input.get(NumberUtils.allSameKey("DOMAINKEY")).object();
			List<FutureGet> getData = Collections.synchronizedList(new ArrayList<>());
			List<FuturePut> putWords = Collections.synchronizedList(new ArrayList<>());

			Number160 domainKey = Number160.createHash(dht.peerDHT().peerID() + "_" + System.currentTimeMillis());

			Set<String> words2 = Collections.synchronizedSet(new HashSet<>());
			Map<String, Integer> tmpRes = Collections.synchronizedMap(new HashMap<String, Integer>());
			for (String word : words) {
				Number160 wordKeyHash = Number160.createHash(word);
				getData.add(dht.getAll(wordKeyHash, receivedDomainKey).addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							words2.add(word);
							int sum = 0;
							Set<Number640> keySet = future.dataMap().keySet();
							for (Number640 k : keySet) {
								sum += (Integer) ((Value) future.dataMap().get(k).object()).value();
							}
							tmpRes.put(word, sum);
							// logger.info("before " + tmpRes);

							byte[] serializedMap = SerializeUtils.serializeJavaObject(tmpRes);
							if (serializedMap.length >= FileSize.EIGHT_KILO_BYTES.value()) {
								synchronized (tmpRes) {
									for (String word : tmpRes.keySet()) {
										putWords.add(dht.put(Number160.createHash(word), tmpRes.get(word), domainKey));
									}
									words2.addAll(tmpRes.keySet());
									tmpRes.clear();
								}
							}
							// logger.info("after " + tmpRes);
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
						// logger.info("before x " + tmpRes);
						if (tmpRes.size() > 0) {
							synchronized (tmpRes) {
								for (String word : tmpRes.keySet()) {
									putWords.add(dht.put(Number160.createHash(word), tmpRes.get(word), domainKey));
								}
								words2.addAll(tmpRes.keySet());
								tmpRes.clear();
							}
						}
						// logger.info("after x " + tmpRes);
						Futures.whenAllSuccess(putWords).addListener(new BaseFutureAdapter<BaseFuture>() {

							@Override
							public void operationComplete(BaseFuture future) throws Exception {
								if (future.isSuccess()) {
									// logger.info("broadcast");
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
		private static Logger logger = LoggerFactory.getLogger(PrintTask.class);

		public PrintTask(Number640 previousId, Number640 currentId) {
			super(previousId, currentId);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = -8206142810699508919L;

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTWrapper dht) throws Exception {
			logger.info("Executing print task");
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
						logger.info("==========WORDCOUNT RESULTS OF PEER WITH ID: " + dht.peerDHT().peerID().intValue() + "==========");
						logger.info("=====================================");
						for (String word : wordList) {
							logger.info(word + " " + results.get(word));
						}
						logger.info("=====================================");
						NavigableMap<Number640, Data> newInput = new TreeMap<>();
						keepTaskIDs(input, newInput);
						newInput.put(NumberUtils.allSameKey("CURRENTTASK"), input.get(NumberUtils.allSameKey("WRITETASKID")));
						newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
						newInput.put(NumberUtils.allSameKey("SENDERID"), new Data(dht.peerDHT().peerID()));
						dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);

					}
				}
			});
		}

	}

	public static class ShutdownTask extends Task {
		private static Logger logger = LoggerFactory.getLogger(ShutdownTask.class);

		/**
		 * 
		 */
		private static final long serialVersionUID = -5543401293112052880L;

		private int retrievalCounter = 0;
		private int nrOfParticipatingPeers;

		public ShutdownTask(Number640 previousId, Number640 currentId, int nrOfParticipatingPeers) {
			super(previousId, currentId);
			this.nrOfParticipatingPeers = nrOfParticipatingPeers;
		}

		@Override
		public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTWrapper dht) throws Exception {

			if (++retrievalCounter == nrOfParticipatingPeers) {
				logger.info("Received shutdown message. Counter is: " + retrievalCounter);
				new Thread(new Runnable() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						try {
							Thread.sleep(5000);

							// t.shutdown();
							dht.shutdown();
							dht.broadcastHandler().shutdown();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}).start();
			} else {
				logger.info("RetrievalCounter is only: " + retrievalCounter);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		int nrOfParticipatingPeers = 2;

		// String filesPath = new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles/";
		String filesPath = "/home/ozihler/Desktop/files/splitFiles/testfiles";
		Job job = new Job();
		Task startTask = new StartTask(null, NumberUtils.next());
		Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next());
		Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next());
		Task writeTask = new PrintTask(reduceTask.currentId(), NumberUtils.next());
		Task initShutdown = new ShutdownTask(writeTask.currentId(), NumberUtils.next(), nrOfParticipatingPeers);

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

		DHTWrapper dht = DHTWrapper.create("192.168.1.147", 4003, 4004);
		// DHTWrapper dht = DHTWrapper.create("192.168.1.171", 4004, 4004);
		MapReduceBroadcastHandler broadcastHandler = new MapReduceBroadcastHandler(dht);
		dht.broadcastHandler(broadcastHandler);
		dht.connect();

		// job.mapReduceBroadcastHandler(MapReduceBroadcastHandler.class);
		job.start(input, dht);

	}

	private static void keepTaskIDs(NavigableMap<Number640, Data> input, Map<Number640, Data> tmpInput) {
		tmpInput.put(NumberUtils.allSameKey("INPUTTASKID"), input.get(NumberUtils.allSameKey("INPUTTASKID")));
		tmpInput.put(NumberUtils.allSameKey("MAPTASKID"), input.get(NumberUtils.allSameKey("MAPTASKID")));
		tmpInput.put(NumberUtils.allSameKey("REDUCETASKID"), input.get(NumberUtils.allSameKey("REDUCETASKID")));
		tmpInput.put(NumberUtils.allSameKey("WRITETASKID"), input.get(NumberUtils.allSameKey("WRITETASKID")));
		tmpInput.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
	}
}
