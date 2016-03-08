package net.tomp2p.mapreduce.examplejob;

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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class ReduceTask extends Task {
	/**
	* 
	*/
	private static final long serialVersionUID = -5662749658082184304L;
	private static Logger logger = LoggerFactory.getLogger(ReduceTask.class);
	public static long cntr = 0;

	private static final int NUMBER_OF_EXECUTIONS = 1;
	private static Map<Number160, Set<Number160>> aggregatedFileKeys = Collections.synchronizedMap(new HashMap<>());
	private static Map<String, Integer> reduceResults = Collections.synchronizedMap(new HashMap<>()); // First Integer in Map<Integer...> is to say which domainKey index (0, 1, ..., NUMBER_OF_EXECUTIONS) --> NOT YET

	public ReduceTask(Number640 previousId, Number640 currentId) {
		super(previousId, currentId);
	}

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		Number640 inputStorageKey = (Number640) input.get(NumberUtils.STORAGE_KEY).object();

		synchronized (aggregatedFileKeys) {
			Set<Number160> domainKeys = aggregatedFileKeys.get(inputStorageKey.locationKey());
			if (domainKeys == null) {
				domainKeys = Collections.synchronizedSet(new HashSet<>());
				aggregatedFileKeys.put(inputStorageKey.locationKey(), domainKeys);
			}
			domainKeys.add(inputStorageKey.domainKey());
		}
		// Need to know how many files, where from? --> user knows it?
		int nrOfFiles = (int) input.get(NumberUtils.allSameKey("NUMBEROFFILES")).object();
		if (nrOfFiles > aggregatedFileKeys.keySet().size()) {
			logger.info("["+this+"] Expecting #[" + nrOfFiles + "], current #[" + aggregatedFileKeys.size() + "]: ");
		} else {
			logger.info("["+this+"] Received all #[" + nrOfFiles + "] files #[" + aggregatedFileKeys.size() + "]: Start executing Reduce");
			synchronized (aggregatedFileKeys) {
				for (Number160 locationKey : aggregatedFileKeys.keySet()) {
					int domainKeySize = aggregatedFileKeys.get(locationKey).size();
					if (domainKeySize < NUMBER_OF_EXECUTIONS) {
						logger.info("Expecting [" + NUMBER_OF_EXECUTIONS + "] number of executions, currently holding: [" + domainKeySize + "] domainkeys for this locationkey");
						return;
					}
				}
			}

			List<FutureDone<Void>> getData = Collections.synchronizedList(new ArrayList<>());
			// Only here, when all the files were prepared, the reduce task is executed
			logger.info("Executing Reduce Task");
			final int max = aggregatedFileKeys.keySet().size();
			final AtomicInteger counter = new AtomicInteger(0);
			final FutureDone<Void> fd = new FutureDone<>();
			synchronized (aggregatedFileKeys) {

				for (Number160 locationKey : aggregatedFileKeys.keySet()) {
					Set<Number160> domainKeys = aggregatedFileKeys.get(locationKey);
					// int index = 0;
					System.err.println("Domainkeys: " + domainKeys);
					for (Number160 domainKey : domainKeys) {// Currently, only one final result.
						// Map<String, Integer> reduceResults = fileResults.get(index);
						// ++index;
						getData.add(pmr.get(locationKey, domainKey, input).start().addListener(new BaseFutureAdapter<FutureTask>() {

							@Override
							public void operationComplete(FutureTask future) throws Exception {
								if (future.isSuccess()) {
									Map<String, Integer> fileResults = (Map<String, Integer>) future.data().object();
									synchronized (reduceResults) {
										for (String word : fileResults.keySet()) {
											Integer sum = reduceResults.get(word);
											if (sum == null) {
												sum = 0;
											}

											Integer fileCount = fileResults.get(word);
											sum += fileCount;
											reduceResults.put(word, sum);

										}
									}
								} else {
									logger.info("Could not acquire locKey[" + locationKey.intValue() + "], domainkey[" + domainKey.intValue() + "]");
								}
								if (counter.incrementAndGet() == max) {// TODO: be aware if futureGet fails, this will be set to true although it failed --> result will be wrong!!!
									fd.done();
								}
							}

						}));
						break;
					}
				}
			}

			fd.addListener(new BaseFutureAdapter<BaseFuture>() {

				@Override
				public void operationComplete(BaseFuture future) throws Exception {
					if (future.isSuccess()) {
						// logger.info("broadcast");
						Number160 resultKey = Number160.createHash("FINALRESULT");

						Number160 outputDomainKey = Number160.createHash(pmr.peer().peerID() + "_" + (cntr++));
						Number640 storageKey = new Number640(resultKey, outputDomainKey, Number160.ZERO, Number160.ZERO);
						pmr.put(resultKey, outputDomainKey, reduceResults, 1).start().addListener(new BaseFutureAdapter<FutureTask>() {

							@Override
							public void operationComplete(FutureTask future) throws Exception {
								if (future.isSuccess()) {
									NavigableMap<Number640, Data> newInput = new TreeMap<>();
									keepInputKeyValuePairs(input, newInput, new String[] {"JOB_KEY", "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID" });
									newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
									newInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("REDUCETASKID")));
									newInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("WRITETASKID")));
									newInput.put(NumberUtils.STORAGE_KEY, new Data(storageKey));
									pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();
								}
							}
						});
					}
				}

			});
		}
	}

}