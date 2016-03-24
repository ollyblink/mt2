package net.tomp2p.mapreduce.examplejob;

import java.io.BufferedWriter;
import java.io.File;
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

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.TestInformationGatherUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class PrintTask extends Task {
	private static int counter = 0;

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
		int execID = counter++;

		TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> START EXECUTING PRINTTASK [" + execID + "] for job ["+input.get(NumberUtils.JOB_ID)+"]");
		if (finished.get() || isBeingExecuted.get()) {
			logger.info("Already executed/Executing reduce results >> ignore call");
			TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> RETURNED EXECUTING PRINTTASK [" + execID + "]");

			return;
		}

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
						printResults("temp_["+reduceResults.keySet().size()+"]words_[" + DateFormat.getDateTimeInstance().format(new Date()) + "]", reduceResults, pmr.peer().peerID().intValue());
						NavigableMap<Number640, Data> newInput = new TreeMap<>();
						keepInputKeyValuePairs(input, newInput, new String[] { "JOB_KEY", "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID" });
						newInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("WRITETASKID")));
						newInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
						newInput.put(NumberUtils.INPUT_STORAGE_KEY, input.get(NumberUtils.OUTPUT_STORAGE_KEY));
						Number640 o = new Number640(new Random());
//						newInput.put(NumberUtils.OUTPUT_STORAGE_KEY, input.get(NumberUtils.OUTPUT_STORAGE_KEY)); //Below replaces this because bc handler has a set with msgs and would not execute it if the outputkey was the same (see bchandler)
						newInput.put(NumberUtils.OUTPUT_STORAGE_KEY, new Data(o));

						newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
						// newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
						finished.set(true);
						logger.info(">>>>>>>>>>>>>>>>>>>> FINISHED EXECUTING PRINTTASK [" + execID + "] with [" + reduceResults.keySet().size() + "] words");
						TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> FINISHED EXECUTING PRINTTASK [" + execID + "] with [" + reduceResults.keySet().size() + "] words");
//						TestInformationGatherUtils.writeOut();
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
	public static void main(String[] args) {
		System.err.println((Integer.MAX_VALUE/1000)/60/60);
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