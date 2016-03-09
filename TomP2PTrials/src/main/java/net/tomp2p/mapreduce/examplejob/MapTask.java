package net.tomp2p.mapreduce.examplejob;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MapTask extends Task {
	private static Logger logger = LoggerFactory.getLogger(MapTask.class);
	// public static long cntr = 0;
	int nrOfExecutions = 2;

	public MapTask(Number640 previousId, Number640 currentId) {
		super(previousId, currentId);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 7150229043957182808L;

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		logger.info(">>>>>>>>>>>>>>>>>>>> Executing Map Task");
		Number640 inputStorageKey = (Number640) input.get(NumberUtils.OUTPUT_STORAGE_KEY).object();
		Number160 outputLocationKey = inputStorageKey.locationKey();
		Number160 outputDomainKey = Number160.createHash(pmr.peer().peerID() + "_" + (new Random().nextLong()));
		pmr.get(inputStorageKey.locationKey(), inputStorageKey.domainKey(), input).start().addListener(new BaseFutureAdapter<FutureTask>() {

			@Override
			public void operationComplete(FutureTask future) throws Exception {
				if (future.isSuccess()) {
					String text = ((String) future.data().object()).replaceAll("[\t\n\r]", " ");
					String[] ws = text.split(" ");

					Map<String, Integer> fileResults = new HashMap<String, Integer>();
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
					logger.info("MapTASK: input was[" + text + "], produced output[" + fileResults + "]");
					pmr.put(outputLocationKey, outputDomainKey, fileResults, nrOfExecutions).start().addListener(new BaseFutureAdapter<BaseFuture>() {

						@Override
						public void operationComplete(BaseFuture future) throws Exception {
							if (future.isSuccess()) {
								NavigableMap<Number640, Data> newInput = new TreeMap<>();
								keepInputKeyValuePairs(input, newInput, new String[] { "JOB_KEY", "NUMBEROFFILES", "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID" });
								newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
								newInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("MAPTASKID")));
								newInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
								newInput.put(NumberUtils.INPUT_STORAGE_KEY, input.get(NumberUtils.OUTPUT_STORAGE_KEY));
								newInput.put(NumberUtils.OUTPUT_STORAGE_KEY, new Data(new Number640(outputLocationKey, outputDomainKey, Number160.ZERO, Number160.ZERO)));
								pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();

							} else {
								logger.info("!future.isSuccess(), failed reason: " + future.failedReason());
							}
						}
					});
					// logger.info("After: nr of words " + words.size());
				} else {// Do nothing
					logger.info("!future.isSuccess(), failed reason: " + future.failedReason());
				}
			}

		});
	}

}