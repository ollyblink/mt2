package net.tomp2p.mapreduce;

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
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class Main {
	public static void main(String[] args) throws Exception {
		Task startTask = new Task(null, NumberUtils.next()) {

			/**
			 * 
			 */
			private static final long serialVersionUID = -5879889214195971852L;

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht)
					throws Exception {

				final List<FuturePut> futurePuts = Collections.synchronizedList(new ArrayList<>());
				// Put data
				String text1 = (String) input.get(NumberUtils.allSameKey("DATA1")).object();
				String text2 = (String) input.get(NumberUtils.allSameKey("DATA2")).object();
				Number160 dataKey1 = Number160.createHash(text1);
				Number160 dataKey2 = Number160.createHash(text2);

				futurePuts.add(dht.put(dataKey1, text1));
				futurePuts.add(dht.put(dataKey2, text2));

				// Put job
				Number160 jobKey = Number160.createHash("JOBKEY");
				futurePuts.add(dht.put(jobKey, input.get(NumberUtils.allSameKey("JOBKEY"))));

				Futures.whenAllSuccess(futurePuts).addListener(new BaseFutureAdapter<BaseFuture>() {

					@Override
					public void operationComplete(BaseFuture future) throws Exception {
						if (future.isSuccess()) {
							NavigableMap<Number640, Data> newInput = new TreeMap<>();
							keepTaskIDs(input, newInput);
							newInput.put(NumberUtils.allSameKey("NEXTTASK"),
									input.get(NumberUtils.allSameKey("MAPTASKID")));
							newInput.put(NumberUtils.allSameKey("DATA1"), new Data(dataKey1));
							newInput.put(NumberUtils.allSameKey("DATA2"), new Data(dataKey2));
							newInput.put(NumberUtils.allSameKey("JOBKEY"), new Data(jobKey));
							dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
						} else {
							// Do nothing
						}
					}

				});
			}

		};

		Task mapTask = new Task(startTask.currentId(), NumberUtils.next()) {

			/**
			 * 
			 */
			private static final long serialVersionUID = 7150229043957182808L;

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht)
					throws Exception {

				Number160 dataKey1 = (Number160) input.get(NumberUtils.allSameKey("DATA1")).object();
				Number160 dataKey2 = (Number160) input.get(NumberUtils.allSameKey("DATA2")).object();
				List<Number160> allDataKeys = Collections.synchronizedList(new ArrayList<>());
				allDataKeys.add(dataKey1);
				allDataKeys.add(dataKey2);
				List<FutureGet> getData = Collections.synchronizedList(new ArrayList<>());
				List<FuturePut> putWords = Collections.synchronizedList(new ArrayList<>());
				Set<String> words = Collections.synchronizedSet(new HashSet<>());
				Number160 domainKey = Number160
						.createHash(dht.peerDHT().peer().peerID() + "_" + System.currentTimeMillis());

				for (Number160 dataKey : allDataKeys) {
					getData.add(dht.get(dataKey).addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								String text = (String) future.data().object();
								String[] ws = text.split(" ");
								for (String word : ws) {
									words.add(word);
									putWords.add(dht.addAsList(Number160.createHash(word), 1, domainKey));
									System.out.println("Adding " + word + ", " + 1);
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
										newInput.put(NumberUtils.allSameKey("NEXTTASK"),
												input.get(NumberUtils.allSameKey("REDUCETASKID")));
										newInput.put(NumberUtils.allSameKey("WORDS"), new Data(words));
										newInput.put(NumberUtils.allSameKey("DOMAINKEY"), new Data(domainKey));
										dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
									}
								}
							});
						}
					}

				});

			}

		};

		Task reduceTask = new Task(mapTask.currentId(), NumberUtils.next()) {

			/**
			 * 
			 */
			private static final long serialVersionUID = -5662749658082184304L;

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht)
					throws Exception {

				Set<String> words = (Set<String>) input.get(NumberUtils.allSameKey("WORDS")).object();
				Number160 receivedDomainKey = (Number160) input.get(NumberUtils.allSameKey("DOMAINKEY")).object();
				List<FutureGet> getData = Collections.synchronizedList(new ArrayList<>());
				List<FuturePut> putWords = Collections.synchronizedList(new ArrayList<>());

				Number160 domainKey = Number160
						.createHash(dht.peerDHT().peer().peerID() + "_" + System.currentTimeMillis());

				Set<String> words2 = Collections.synchronizedSet(new HashSet<>());
				for (String word : words) {
					Number160 wordKeyHash = Number160.createHash(word);
					getData.add(
							dht.getAll(wordKeyHash, receivedDomainKey).addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								// for (Number640 key :
								// future.dataMap().keySet()) {
								// Object o =
								// future.dataMap().get(key).object();
								// System.out.println("For word " + word + "
								// retrieved " + o + " of type "
								// + o.getClass().getSimpleName());
								// }
								words2.add(word);
								int sum = future.dataMap().keySet().size();
								System.out.println("Adding " + word + ", " + sum);
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
										newInput.put(NumberUtils.allSameKey("NEXTTASK"),
												input.get(NumberUtils.allSameKey("WRITETASKID")));
										newInput.put(NumberUtils.allSameKey("WORDS"), new Data(words2));
										newInput.put(NumberUtils.allSameKey("DOMAIN"), new Data(domainKey));
										dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
									}
								}
							});
						}
					}
				});
			}

		};

		Task writeTask = new Task(reduceTask.currentId(), NumberUtils.next()) {

			/**
			 * 
			 */
			private static final long serialVersionUID = -8206142810699508919L;

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht)
					throws Exception {

				Set<String> words = (Set<String>) input.get(NumberUtils.allSameKey("WORDS")).object();
				Number160 receivedDomainKey = (Number160) input.get(NumberUtils.allSameKey("DOMAIN")).object();
				List<FutureGet> getData = Collections.synchronizedList(new ArrayList<>());

				final Map<String, Integer> results = Collections.synchronizedMap(new HashMap<>());
				for (String word : words) {
					getData.add(dht.get(Number160.createHash(word), receivedDomainKey)
							.addListener(new BaseFutureAdapter<FutureGet>() {

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
							System.out.println("==========WORDCOUNT RESULTS==========");
							System.out.println("=====================================");
							for (String word : wordList) {
								System.out.println(word + " " + results.get(word));
							}
							System.out.println("=====================================");
							NavigableMap<Number640, Data> newInput = new TreeMap<>();
							keepTaskIDs(input, newInput);
							newInput.put(NumberUtils.allSameKey("NEXTTASK"),
									input.get(NumberUtils.allSameKey("SHUTDOWNTASKID"))); 
							dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);

						}
					}
				});

			}

		};

		Task initShutdown = new Task(writeTask.currentId(), NumberUtils.next()) {

			/**
			 * 
			 */
			private static final long serialVersionUID = -5543401293112052880L;

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input, DHTConnectionProvider dht)
					throws Exception {
				dht.shutdown();
				System.out.println("Successfully shutdown dht");
			}

		};

		Job job = new Job();
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
		input.put(NumberUtils.allSameKey("DATA1"), new Data(
				"this is a text file a text file with many words as you can imagine and i like to point out that it actually works."));
		input.put(NumberUtils.allSameKey("DATA2"),
				new Data("hello world hello world hello world this is a hello world example with hello world things"));
		input.put(NumberUtils.allSameKey("JOBKEY"), new Data(job.serialize()));

		DHTConnectionProvider dht = DHTConnectionProvider.create("192.168.1.172", 4000, 4000);
		dht.broadcastHandler(new MapReduceBroadcastHandler(dht));
		dht.connect();
		job.start(input, dht);
		while (!dht.peerDHT().peer().isShutdown()) {
			System.out.println("Waiting");
			Thread.sleep(1000);
		}

	}

	private static void keepTaskIDs(NavigableMap<Number640, Data> input, NavigableMap<Number640, Data> newInput) {
		newInput.put(NumberUtils.allSameKey("INPUTTASKID"), input.get(NumberUtils.allSameKey("INPUTTASKID")));
		newInput.put(NumberUtils.allSameKey("MAPTASKID"), input.get(NumberUtils.allSameKey("MAPTASKID")));
		newInput.put(NumberUtils.allSameKey("REDUCETASKID"), input.get(NumberUtils.allSameKey("REDUCETASKID")));
		newInput.put(NumberUtils.allSameKey("WRITETASKID"), input.get(NumberUtils.allSameKey("WRITETASKID")));
		newInput.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
	}
}
