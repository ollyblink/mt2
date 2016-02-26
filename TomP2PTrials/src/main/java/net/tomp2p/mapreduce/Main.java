package net.tomp2p.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
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

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
				DHTConnectionProvider dht = (DHTConnectionProvider) input.get(NumberUtils.allSameKey("DHT")).object();

				final List<FuturePut> puts = new ArrayList<>();
				// Put data
				String text1 = (String) input.get(NumberUtils.allSameKey("DATA1")).object();
				String text2 = (String) input.get(NumberUtils.allSameKey("DATA2")).object();
				Number160 dataKey1 = Number160.createHash(text1);
				Number160 dataKey2 = Number160.createHash(text2);

				puts.add(dht.put(dataKey1, text1));
				puts.add(dht.put(dataKey2, text2));

				// Put job
				Number160 jobKey = Number160.createHash("JOBKEY");
				puts.add(dht.put(jobKey, input.get(NumberUtils.allSameKey("JOBKEY"))));

				Futures.whenAllSuccess(puts).addListener(new BaseFutureAdapter<BaseFuture>() {

					@Override
					public void operationComplete(BaseFuture future) throws Exception {
						if (future.isSuccess()) {
							NavigableMap<Number640, Data> newInput = new TreeMap<>();
							keepTaskIDs(input, newInput);
							newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get("MAPTASKID"));
							newInput.put(NumberUtils.allSameKey("DATA1"), new Data(dataKey1));
							newInput.put(NumberUtils.allSameKey("DATA1"), new Data(dataKey2));
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

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
				DHTConnectionProvider dht = (DHTConnectionProvider) input.get(NumberUtils.allSameKey("DHT")).object();

				Number160 dataKey1 = (Number160) input.get(NumberUtils.allSameKey("DATA1")).object();
				Number160 dataKey2 = (Number160) input.get(NumberUtils.allSameKey("DATA2")).object();
				List<Number160> allDataKeys = new ArrayList<>();
				allDataKeys.add(dataKey1);
				allDataKeys.add(dataKey2);
				List<FuturePut> putWords = new ArrayList<>();
				Set<String> words = new HashSet<>();
				Number160 domainKey = Number160
						.createHash(dht.peerDHT().peer().peerID() + "_" + System.currentTimeMillis());

				for (Number160 dataKey : allDataKeys) {
					dht.get(dataKey).addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								String text = (String) future.data().object();
								String[] ws = text.split(" ");
								for (String word : ws) {
									words.add(word);
									putWords.add(dht.addAsList(Number160.createHash(word), new Data(new Integer(1)),
											domainKey));
								}

							} else {
								// Do nothing
							}
						}

					});
				}

				Futures.whenAllSuccess(putWords).addListener(new BaseFutureAdapter<BaseFuture>() {

					@Override
					public void operationComplete(BaseFuture future) throws Exception {
						if (future.isSuccess()) {
							NavigableMap<Number640, Data> newInput = new TreeMap<>();
							keepTaskIDs(input, newInput);
							newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get("REDUCETASKID"));
							newInput.put(NumberUtils.allSameKey("WORDS"), new Data(words));
							newInput.put(NumberUtils.allSameKey("DOMAINKEY"), new Data(domainKey));
							dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
						}
					}
				});
			}

		};

		Task reduceTask = new Task(mapTask.currentId(), NumberUtils.next()) {

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
				DHTConnectionProvider dht = (DHTConnectionProvider) input.get(NumberUtils.allSameKey("DHT")).object();

				Set<String> words = (Set<String>) input.get(NumberUtils.allSameKey("WORDS")).object();
				Number160 receivedDomainKey = (Number160) input.get(NumberUtils.allSameKey("DOMAINKEY")).object();
				List<FuturePut> putWords = new ArrayList<>();
				Number160 domainKey = Number160
						.createHash(dht.peerDHT().peer().peerID() + "_" + System.currentTimeMillis());

				for (String wordKey : words) {
					Number160 wordKeyHash = Number160.createHash(wordKey);
					dht.getAll(wordKeyHash, receivedDomainKey).addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								Integer sum = future.dataMap().keySet().size();
								putWords.add(dht.put(wordKeyHash, new Data(sum), domainKey));
							} else {
								// Do nothing
							}
						}

					});
				}
				Futures.whenAllSuccess(putWords).addListener(new BaseFutureAdapter<BaseFuture>() {

					@Override
					public void operationComplete(BaseFuture future) throws Exception {
						if (future.isSuccess()) {
							NavigableMap<Number640, Data> newInput = new TreeMap<>();
							keepTaskIDs(input, newInput);
							newInput.put(NumberUtils.allSameKey("NEXTTASK"), input.get("WRITETASKID"));
							newInput.put(NumberUtils.allSameKey("WORDS"), new Data(words));
							newInput.put(NumberUtils.allSameKey("DOMAIN"), new Data(domainKey));
							dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
						}
					}
				});
			}

		};

		Task writeTask = new Task(reduceTask.currentId(), NumberUtils.next()) {

			@Override
			public void broadcastReceiver(NavigableMap<Number640, Data> input) throws Exception {
				DHTConnectionProvider dht = (DHTConnectionProvider) input.get(NumberUtils.allSameKey("DHT")).object();

				Set<String> words = (Set<String>) input.get(NumberUtils.allSameKey("WORDS")).object();
				Number160 receivedDomainKey = (Number160) input.get(NumberUtils.allSameKey("DOMAINKEY")).object();

				final Map<String, Integer> results = SyncedCollectionProvider.syncedHashMap();
				List<FutureGet> futureGets = SyncedCollectionProvider.syncedArrayList();
				for (String word : words) {
					futureGets.add(dht.get(Number160.createHash(word), receivedDomainKey)
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

				Futures.whenAllSuccess(futureGets).addListener(new BaseFutureAdapter<BaseFuture>() {

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
						}
					}
				});

			}

		};

		Job job = new Job();
		job.addTask(startTask);
		job.addTask(mapTask);
		job.addTask(reduceTask);
		job.addTask(writeTask);
		DHTConnectionProvider dht = DHTConnectionProvider.create("", 1, 1);
		dht.broadcastHandler(new MapReduceBroadcastHandler(dht));
		dht.connect();

		NavigableMap<Number640, Data> input = new TreeMap<>();
		input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(startTask.currentId()));
		input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(mapTask.currentId()));
		input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(reduceTask.currentId()));
		input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(writeTask.currentId()));
		input.put(NumberUtils.allSameKey("DATA1"), new Data("this is a text file"));
		input.put(NumberUtils.allSameKey("DATA2"), new Data("hello world hello world hello world"));
		input.put(NumberUtils.allSameKey("JOBKEY"), new Data(job.serialize()));
		input.put(NumberUtils.allSameKey("DHT"), new Data(dht));
		job.start(input);
	}

	private static void keepTaskIDs(NavigableMap<Number640, Data> input, NavigableMap<Number640, Data> newInput) {
		newInput.put(NumberUtils.allSameKey("INPUTTASKID"), input.get("INPUTTASKID"));
		newInput.put(NumberUtils.allSameKey("MAPTASKID"), input.get("MAPTASKID"));
		newInput.put(NumberUtils.allSameKey("REDUCETASKID"), input.get("REDUCETASKID"));
		newInput.put(NumberUtils.allSameKey("WRITETASKID"), input.get("WRITETASKID"));
	}
}
