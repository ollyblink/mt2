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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.FutureTask;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.FileSplitter;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TestInformationGatherUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class StartTask extends Task {

	private static int counter = 0;
	private static Logger logger = LoggerFactory.getLogger(StartTask.class);
	// public static long cntr = 0;
	private int nrOfExecutions = 2;

	public StartTask(Number640 previousId, Number640 currentId, int nrOfExecutions) {
		super(previousId, currentId);
		this.nrOfExecutions = nrOfExecutions;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -5879889214195971852L;

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		startTaskCounter.incrementAndGet();
		// "StartTask.broadcastReceiver);
		int execID = counter++;
 		// Number160 jobLocationKey = Number160.createHash("JOBKEY");
		// Number160 jobDomainKey = Number160.createHash("JOBKEY");

		Number160 filesDomainKey = Number160.createHash(pmr.peer().peerID() + "_" + (new Random().nextLong()));

		Number640 jobStorageKey = (Number640) (input.get(NumberUtils.JOB_ID).object());

		// Data jobToPut = input.get(NumberUtils.JOB_DATA);
		// pmr.put(jobStorageKey.locationKey(), jobStorageKey.domainKey(), jobToPut.object(), Integer.MAX_VALUE).start("").addListener(new BaseFutureAdapter<FutureTask>() {
		//
		// @Override
		// public void operationComplete(FutureTask future) throws Exception {
		// if (future.isSuccess()) {
		// logger.info("Sucess on put(Job) with key " + jobStorageKey.locationAndDomainKey().intValue() + ", continue to put data for job");
		logger.info(">>>>>>>>>>>>>>>>>>>> EXECUTING START TASK [" + execID + "]");
		// =====END NEW BC DATA===========================================================
		Map<Number640, Data> tmpNewInput = Collections.synchronizedMap(new TreeMap<>()); // Only used to avoid adding it in each future listener...
		keepInputKeyValuePairs(input, tmpNewInput, new String[] { "JOB_KEY", "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID", "NUMBEROFFILES" });
		tmpNewInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
		tmpNewInput.put(NumberUtils.RECEIVERS, input.get(NumberUtils.RECEIVERS));
		tmpNewInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("INPUTTASKID")));
		tmpNewInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("MAPTASKID")));
		// tmpNewInput.put(NumberUtils.JOB_DATA, new Data(jobStorageKey));

		// tmpNewInput.put(NumberUtils.allSameKey("NUMBEROFFILES"), new Data(input.get(Number)));
		// =====END NEW BC DATA===========================================================

		// ============GET ALL THE FILES ==========
		String filesPath = (String) input.get(NumberUtils.allSameKey("DATAFILEPATH")).object();
		List<String> pathVisitor = Collections.synchronizedList(new ArrayList<>());
		FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
		// ===== FINISHED GET ALL THE FILES =================

		// ===== SPLIT AND DISTRIBUTE ALL THE DATA ==========
		final List<FutureTask> futurePuts = Collections.synchronizedList(new ArrayList<>());
		// Map<Number160, FutureTask> all = Collections.synchronizedMap(new HashMap<>());
		int nrOfFiles = (int) input.get(NumberUtils.allSameKey("NUMBEROFFILES")).object();
		ThreadPoolExecutor e = new ThreadPoolExecutor(nrOfFiles, nrOfFiles, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>());
		AtomicInteger cntr = new AtomicInteger(0);
		for (String filePath : pathVisitor) {
			e.submit(new Runnable() {

				@Override
				public void run() {
					try {

						Map<Number160, FutureTask> tmp = FileSplitter.splitWithWordsAndWrite(filePath, pmr, nrOfExecutions, filesDomainKey, FileSize.THIRTY_TWO_MEGA_BYTES.value(), "UTF-8");
						TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> EXECUTING START TASK [" + execID + "]");

						futurePuts.addAll(tmp.values());
						TestInformationGatherUtils.addLogEntry("File path: " + filePath);
						logger.info("File path: " + filePath);
						for (Number160 fileKey : tmp.keySet()) {
							tmp.get(fileKey).addListener(new BaseFutureAdapter<BaseFuture>() {

								@Override
								public void operationComplete(BaseFuture future) throws Exception {
									if (future.isSuccess()) {
										// for (Number160 fileKey : all.keySet()) {

										Number640 storageKey = new Number640(fileKey, filesDomainKey, Number160.ZERO, Number160.ZERO);

										if (future.isSuccess()) {
											NavigableMap<Number640, Data> newInput = new TreeMap<>();
											synchronized (tmpNewInput) {
												newInput.putAll(tmpNewInput);
											}
											// newInput.put(NumberUtils.INPUT_STORAGE_KEY, new Data(null)); //Don't need it, as there is no input key.. first task
											newInput.put(NumberUtils.OUTPUT_STORAGE_KEY, new Data(storageKey));
											// Here: instead of futures when all, already send out broadcast
											logger.info("success on put(k[" + storageKey.locationKey().intValue() + "], v[content of ()])");

											pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();
//											pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();

											finishedTaskCounter.incrementAndGet();
											TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> FINISHED EXECUTING STARTTASK [" + execID + "]");

										} else {
											logger.info("No success on put(fileKey, actualValues) for key " + storageKey.locationAndDomainKey().intValue());
										}
									}
								}
							});
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			});

		}
		// Futures.whenAllSuccess(futurePuts).awaitUninterruptibly();

		// } else {
		// logger.info("No sucess on put(Job): Fail reason: " + future.failedReason());
		// }
		// }
		// });

		// Futures.whenAllSuccess(initial);
	}

}