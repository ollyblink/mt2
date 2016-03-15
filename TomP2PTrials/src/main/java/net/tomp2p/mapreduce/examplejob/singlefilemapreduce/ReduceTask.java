package net.tomp2p.mapreduce.examplejob.singlefilemapreduce;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.TestInformationGatherUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

public class ReduceTask extends Task {
	/**
	* 
	*/
	private static final long serialVersionUID = -5662749658082184304L;
	private static Logger logger = LoggerFactory.getLogger(ReduceTask.class);
	// public static long cntr = 0;
	private static AtomicBoolean finished = new AtomicBoolean(false);
	private static AtomicBoolean isBeingExecuted = new AtomicBoolean(false);

	private int nrOfExecutions;
	int nrOfRetrievals = Integer.MAX_VALUE; // Doesn't matter...

	private static Map<Number160, Set<Number160>> aggregatedFileKeys = Collections.synchronizedMap(new HashMap<>());
	private static Map<String, Integer> reduceResults = Collections.synchronizedMap(new HashMap<>()); // First Integer in Map<Integer...> is to say which domainKey index (0, 1, ..., NUMBER_OF_EXECUTIONS) --> NOT YET
	private static Map<PeerAddress, Integer> cntr = Collections.synchronizedMap(new HashMap<>());

	public ReduceTask(Number640 previousId, Number640 currentId, int nrOfExecutions) {
		super(previousId, currentId);
		this.nrOfExecutions = nrOfExecutions;
	}

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> START EXECUTING REDUCETASK");
		PeerAddress sender = (PeerAddress) input.get(NumberUtils.SENDER).object();

		synchronized (cntr) {
			Integer cnt = cntr.get(sender);
			if (cnt == null) {
				cnt = new Integer(0);
			}
			++cnt;
			cntr.put(sender, cnt);
		}
		if (finished.get() || isBeingExecuted.get()) {
			logger.info("Already executed/Executing reduce results >> ignore call");
			return;
		}
		Number640 inputStorageKey = (Number640) input.get(NumberUtils.OUTPUT_STORAGE_KEY).object();

		synchronized (aggregatedFileKeys) {

			logger.info("Added domainkey for location  key [" + inputStorageKey.locationKey() + "] from sender [" + sender.peerId().shortValue() + "]");
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
			logger.info("[" + this + "] Expecting #[" + nrOfFiles + "], current #[" + aggregatedFileKeys.size() + "]: ");
			return;
		} else {
			logger.info("[" + this + "] Received all #[" + nrOfFiles + "] files #[" + aggregatedFileKeys.size() + "]: Check if all data files were executed enough times");
			synchronized (aggregatedFileKeys) {
				for (Number160 locationKey : aggregatedFileKeys.keySet()) {
					int domainKeySize = aggregatedFileKeys.get(locationKey).size();
					if (domainKeySize < nrOfExecutions) {
						logger.info("Expecting [" + nrOfExecutions + "] number of executions, currently holding: [" + domainKeySize + "] domainkeys for this locationkey");
						return;
					}
				}
			}
			isBeingExecuted.set(true);
			logger.info("Expected [" + nrOfExecutions + "] finished executions for all [" + aggregatedFileKeys.size() + "] files and received it. START REDUCING.");
			logger.info("Received the following number of messages from these peers:");
			synchronized (cntr) {
				for (PeerAddress p : cntr.keySet()) {
					logger.info("Peer[" + p.peerId().shortValue() + "] sent [" + cntr.get(p) + "] messages");
				}
			}
			// List<FutureDone<Void>> getData = Collections.synchronizedList(new ArrayList<>());
			// Only here, when all the files were prepared, the reduce task is executed
			final int max = aggregatedFileKeys.keySet().size();
			final AtomicInteger counter = new AtomicInteger(0);
			final FutureDone<Void> fd = new FutureDone<>();
			synchronized (aggregatedFileKeys) {
				for (Number160 locationKey : aggregatedFileKeys.keySet()) {
					logger.info("Domain keys: " + aggregatedFileKeys.get(locationKey));
					// Set<Number160> domainKeys = aggregatedFileKeys.get(locationKey);
					// int index = 0;

					// for (Number160 domainKey : domainKeys) {// Currently, only one final result.
					// Map<String, Integer> reduceResults = fileResults.get(index);
					// ++index;
					Number160 domainKey = aggregatedFileKeys.get(locationKey).iterator().next();
					pmr.get(locationKey, domainKey, input).start().addListener(new BaseFutureAdapter<FutureTask>() {

						@Override
						public void operationComplete(FutureTask future) throws Exception {
							if (future.isSuccess()) {
								synchronized (reduceResults) {
									Map<String, Integer> fileResults = (Map<String, Integer>) future.data().object();
									for (String word : fileResults.keySet()) {
										Integer sum = reduceResults.get(word);
										if (sum == null) {
											sum = 0;
										}

										Integer fileCount = fileResults.get(word);
										sum += fileCount;
										reduceResults.put(word, sum);
									}
									// logger.info("Intermediate reduceResults: " + reduceResults);
								}
							} else {
								logger.info("Could not acquire locKey[" + locationKey.intValue() + "], domainkey[" + domainKey.intValue() + "]");
							}
							// Here I need to inform all about the release of the items again
							// newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
							// newInput.put(NumberUtils.INPUT_STORAGE_KEY, input.get(NumberUtils.OUTPUT_STORAGE_KEY));
							//
							// pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();
							if (counter.incrementAndGet() == max) {// TODO: be aware if futureGet fails, this will be set to true although it failed --> result will be wrong!!!
								fd.done();
							}
						}

					});

					// }
				}

				fd.addListener(new BaseFutureAdapter<BaseFuture>() {

					@Override
					public void operationComplete(BaseFuture future) throws Exception {
						if (future.isSuccess()) {
							// logger.info("broadcast");
							Number160 resultKey = Number160.createHash("FINALRESULT");

							Number160 outputDomainKey = Number160.createHash(pmr.peer().peerID() + "_" + (new Random().nextLong()));
							Number640 storageKey = new Number640(resultKey, outputDomainKey, Number160.ZERO, Number160.ZERO);
							pmr.put(resultKey, outputDomainKey, reduceResults, nrOfRetrievals).start().addListener(new BaseFutureAdapter<FutureTask>() {

								@Override
								public void operationComplete(FutureTask future) throws Exception {
									if (future.isSuccess()) {
										// for (Number160 locationKey : aggregatedFileKeys.keySet()) {
										// for (Number160 domainKey : aggregatedFileKeys.get(locationKey)) {
										NavigableMap<Number640, Data> newInput = new TreeMap<>();
										keepInputKeyValuePairs(input, newInput, new String[] { "JOB_KEY", "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID" });

										newInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("REDUCETASKID")));
										newInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("WRITETASKID")));
										// newInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
										// TODO Here I need to send ALL <locKey,domainKey>, else all gets on these will run out...
										newInput.put(NumberUtils.OUTPUT_STORAGE_KEY, new Data(storageKey));
										newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
										// newInput.put(NumberUtils.INPUT_STORAGE_KEYS, new Data(aggregatedFileKeys));
										// TODO: problem with this implementation: I don't send Input keys (because even here I cannot be sure that all keys are retrieved... better let it dial out such that it is
										finished.set(true);
										TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> FINISHED EXECUTING REDUCETASK");

										pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();

										// }
										// }
									} else {
										// Do nothing.. timeout will take care of it
									}
								}
							});
						}
					}

				});
			}
		}
	}

}