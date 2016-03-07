package net.tomp2p.mapreduce.examplejob;

import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class PrintTask extends Task {
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
		Number640 storageKey = (Number640) input.get(NumberUtils.STORAGE_KEY).object();
		dht.get(storageKey.locationKey(), storageKey.domainKey(), input).addListener(new BaseFutureAdapter<FutureTask>() {

			@Override
			public void operationComplete(FutureTask future) throws Exception {
				if (future.isSuccess()) {
					HashMap<String, Integer> reduceResults = (HashMap<String, Integer>) future.data().object();
					logger.info("==========WORDCOUNT RESULTS OF PEER WITH ID: " + dht.peer().peerID().intValue() + "==========");
					logger.info("=====================================");
					for (String word : reduceResults.keySet()) {
						logger.info(word + " " + reduceResults.get(word));
					}
					logger.info("=====================================");
					NavigableMap<Number640, Data> newInput = new TreeMap<>();
					keepInputKeyValuePairs(input, newInput, new String[] { "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID" });
					newInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("WRITETASKID")));
					newInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
					dht.broadcast(Number160.createHash(new Random().nextLong()), newInput);
				} else {
					// Do nothing
				}
			}

		});

	}

}