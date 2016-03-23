package net.tomp2p.mapreduce.examplejob;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.FileSplitter;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class StartTask2 extends Task {
	private static Logger logger = LoggerFactory.getLogger(StartTask.class);
	// public static long cntr = 0;
	private int nrOfExecutions;
	private int nrOfFiles;

	public StartTask2(Number640 previousId, Number640 currentId, int nrOfFiles, int nrOfExecutions) {
		super(previousId, currentId);
		this.nrOfFiles = nrOfFiles;
		this.nrOfExecutions = nrOfExecutions;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -5879889214195971852L;

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		Number160 filesDomainKey = Number160.createHash(pmr.peer().peerID() + "_" + (new Random().nextLong()));
		Number640 jobStorageKey = new Number640(Number160.createHash("JOBKEY"), Number160.createHash("JOBKEY"), Number160.ZERO, Number160.ZERO);

		logger.info(">>>>>>>>>>>>>>>>>>>> EXECUTING START TASK");
		// =====END NEW BC DATA===========================================================
		NavigableMap<Number640, Data> tmpNewInput = new TreeMap<>(); // Only used to avoid adding it in each future listener...
		keepInputKeyValuePairs(input, tmpNewInput, new String[] { "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID" });
		tmpNewInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
		tmpNewInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("INPUTTASKID")));
		tmpNewInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("MAPTASKID")));
		tmpNewInput.put(NumberUtils.JOB_KEY, new Data(jobStorageKey));
		tmpNewInput.put(NumberUtils.allSameKey("NUMBEROFFILES"), new Data(nrOfFiles));

		// Add receiver to handle BC messages (job specific handler, defined by user)
		ExampleJobBroadcastReceiver r = new ExampleJobBroadcastReceiver();
		Map<String, byte[]> bcClassFiles = SerializeUtils.serializeClassFile(ExampleJobBroadcastReceiver.class);
		String bcClassName = ExampleJobBroadcastReceiver.class.getName();
		byte[] bcObject = SerializeUtils.serializeJavaObject(r);
		TransferObject t = new TransferObject(bcObject, bcClassFiles, bcClassName);
		List<TransferObject> broadcastReceivers = new ArrayList<>();
		broadcastReceivers.add(t);
		tmpNewInput.put(NumberUtils.RECEIVERS, new Data(broadcastReceivers));
		// =====END NEW BC DATA===========================================================

		// ===== SPLIT AND DISTRIBUTE ALL THE DATA ==========

		String filePath = (String) input.get(NumberUtils.allSameKey("FILEPATH")).object();
		logger.info("Putting " +filePath);
		Map<Number160, FutureTask> tmp = FileSplitter.splitWithWordsAndWrite(filePath, pmr, nrOfExecutions, filesDomainKey, FileSize.MEGA_BYTE.value(), "UTF-8");
		for (Number160 fileKey : tmp.keySet()) {
			tmp.get(fileKey).addListener(new BaseFutureAdapter<BaseFuture>() {

				@Override
				public void operationComplete(BaseFuture future) throws Exception {
					if (future.isSuccess()) {
						// for (Number160 fileKey : all.keySet()) {

						Number640 storageKey = new Number640(fileKey, filesDomainKey, Number160.ZERO, Number160.ZERO);

						if (future.isSuccess()) {

							// newInput.put(NumberUtils.INPUT_STORAGE_KEY, new Data(null)); //Don't need it, as there is no input key.. first task
							tmpNewInput.put(NumberUtils.OUTPUT_STORAGE_KEY, new Data(storageKey));
							// Here: instead of futures when all, already send out broadcast
							logger.info("success on put(k[" + storageKey.locationAndDomainKey().intValue() + "], v[content of ()])");

							pmr.peer().broadcast(new Number160(new Random())).dataMap(tmpNewInput).start();

						} else {
							logger.info("No success on put(fileKey, actualValues) for key " + storageKey.locationAndDomainKey().intValue());
						}
					}
				}
			});
		}

	}

}