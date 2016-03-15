package net.tomp2p.mapreduce.examplejob;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.utils.FileUtils;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class PrintTask extends Task {
	private static Logger logger = LoggerFactory.getLogger(PrintTask.class);
	private static AtomicBoolean finished = new AtomicBoolean(false);
	private static AtomicBoolean isBeingExecuted = new AtomicBoolean(false);

	public PrintTask(Number640 previousId, Number640 currentId) {
		super(previousId, currentId);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -8206142810699508919L;

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		if (finished.get() || isBeingExecuted.get()) {
			logger.info("Already executed/Executing reduce results >> ignore call");
			return;
		}
		TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> START EXECUTING PRINTTASK");

		isBeingExecuted.set(true);
		Data storageKeyData = input.get(NumberUtils.OUTPUT_STORAGE_KEY);
		if (storageKeyData != null) {
			Number640 storageKey = (Number640) storageKeyData.object();
			pmr.get(storageKey.locationKey(), storageKey.domainKey(), input).start().addListener(new BaseFutureAdapter<FutureTask>() {

				@Override
				public void operationComplete(FutureTask future) throws Exception {
					if (future.isSuccess()) {
						Map<String, Integer> reduceResults = new TreeMap<>((Map<String, Integer>) future.data().object());
						// logger.info("==========WORDCOUNT RESULTS OF PEER WITH ID: " + pmr.peer().peerID().intValue() + ", time [" + time + "]==========");
						// logger.info("=====================================");
						// for (String word : reduceResults.keySet()) {
						// logger.info(word + " " + reduceResults.get(word));
						// }
						// logger.info("=====================================");
						printResults("temp", reduceResults, pmr.peer().peerID().intValue());
						NavigableMap<Number640, Data> newInput = new TreeMap<>();
						keepInputKeyValuePairs(input, newInput, new String[] { "JOB_KEY", "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID" });
						newInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("WRITETASKID")));
						newInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
						newInput.put(NumberUtils.INPUT_STORAGE_KEY, input.get(NumberUtils.OUTPUT_STORAGE_KEY));
						newInput.put(NumberUtils.OUTPUT_STORAGE_KEY, input.get(NumberUtils.OUTPUT_STORAGE_KEY));
						newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
						// newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
						finished.set(true);
						TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> FINISHED EXECUTING PRINTTASK");
						pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();
					} else {
						// Do nothing
					}
				}

			});
		} else {
			logger.info("Ignored");
		}

	}

	public static void printResults(String filename, Map<String, Integer> reduceResults, int peerId) throws Exception {
		File f = new File(filename);
		if (f.exists()) {
			f.delete();
		}
		f.createNewFile();

		Path file = Paths.get(filename);
		try (BufferedWriter writer = Files.newBufferedWriter(file, Charset.defaultCharset(), StandardOpenOption.APPEND)) {
			writer.write("==========WORDCOUNT RESULTS OF PEER WITH ID: " + peerId + ", #words [" + reduceResults.keySet().size() + "] time [" + DateFormat.getDateTimeInstance().format(new Date()) + "]==========");
			writer.newLine();

			for (String word : reduceResults.keySet()) {
				writer.write(word + ", " + reduceResults.get(word));
				writer.newLine();
			}
			writer.write("=====================================");
			writer.newLine();
			writer.close();
		}
	}
}