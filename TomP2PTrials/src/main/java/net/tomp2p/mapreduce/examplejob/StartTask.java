package net.tomp2p.mapreduce.examplejob;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

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

public class StartTask extends Task {
	private static Logger logger = LoggerFactory.getLogger(StartTask.class);
	public static long cntr = 0;

	public StartTask(Number640 previousId, Number640 currentId) {
		super(previousId, currentId);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -5879889214195971852L;

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {

		Number160 jobLocationKey = Number160.createHash("JOBKEY");
		Number160 jobDomainKey = Number160.createHash(pmr.peer().peerID() + "_" + (cntr++));

		Number640 jobStorageKey = new Number640(jobLocationKey, jobDomainKey, Number160.ZERO, Number160.ZERO);
		int nrOfExecutions = 1;
		int nrOfFiles = 3;

		Data jobToPut = input.get(NumberUtils.JOB_KEY);
		pmr.put(jobLocationKey, jobDomainKey, jobToPut.object(), Integer.MAX_VALUE).start().addListener(new BaseFutureAdapter<FutureTask>() {

			@Override
			public void operationComplete(FutureTask future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Sucess on put(Job) with key " + jobLocationKey + ", continue to put data for job");
					// =====END NEW BC DATA===========================================================
					Map<Number640, Data> tmpNewInput = Collections.synchronizedMap(new TreeMap<>()); // Only used to avoid adding it in each future listener...
					keepInputKeyValuePairs(input, tmpNewInput, new String[] { "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID" });
					tmpNewInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
					tmpNewInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("INPUTTASKID")));
					tmpNewInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("MAPTASKID")));
					tmpNewInput.put(NumberUtils.JOB_KEY, new Data(jobStorageKey));
					tmpNewInput.put(NumberUtils.allSameKey("NUMBEROFFILES"), new Data(nrOfFiles));

					// Add receiver to handle BC messages (job specific handler, defined by user)
					SimpleBroadcastReceiver r = new SimpleBroadcastReceiver();
					Map<String, byte[]> bcClassFiles = SerializeUtils.serializeClassFile(SimpleBroadcastReceiver.class);
					String bcClassName = SimpleBroadcastReceiver.class.getName();
					byte[] bcObject = SerializeUtils.serializeJavaObject(r);
					TransferObject t = new TransferObject(bcObject, bcClassFiles, bcClassName);
					List<TransferObject> broadcastReceivers = new ArrayList<>();
					broadcastReceivers.add(t);
					tmpNewInput.put(NumberUtils.RECEIVERS, new Data(broadcastReceivers));
					// =====END NEW BC DATA===========================================================

					// ============GET ALL THE FILES ==========
					String filesPath = (String) input.get(NumberUtils.allSameKey("DATAFILEPATH")).object();
					List<String> pathVisitor = Collections.synchronizedList(new ArrayList<>());
					FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
					// ===== FINISHED GET ALL THE FILES =================

					// ===== SPLIT AND DISTRIBUTE ALL THE DATA ==========
					final List<FutureTask> futurePuts = Collections.synchronizedList(new ArrayList<>());
					for (String filePath : pathVisitor) {
						Map<Number160, FutureTask> tmp = FileSplitter.splitWithWordsAndWrite(filePath, pmr, nrOfExecutions, jobDomainKey, FileSize.MEGA_BYTE.value(), "UTF-8");
						for (Number160 fileKey : tmp.keySet()) {
							tmp.get(fileKey).addListener(new BaseFutureAdapter<FutureTask>() {

								@Override
								public void operationComplete(FutureTask future) throws Exception {
									if (future.isSuccess()) {
										NavigableMap<Number640, Data> newInput = new TreeMap<>();
										synchronized (tmpNewInput) {
											newInput.putAll(tmpNewInput);
										}
										newInput.put(NumberUtils.STORAGE_KEY, new Data(new Number640(fileKey, jobDomainKey, Number160.ZERO, Number160.ZERO)));
										// Here: instead of futures when all, already send out broadcast
										pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();

										logger.info("success on put(fileKey, actualValues) and broadcast for key: " + fileKey);
									} else {
										logger.info("No success on put(fileKey, actualValues) for key " + fileKey);
									}
								}

							});
						}
						// fileKeys.addAll(tmp.keySet());
						futurePuts.addAll(tmp.values());
					}
					// logger.info("File keys size:" + fileKeys.size());
					// Just for information! Has no actual value only for the user to be informed if something went wrong, although this will already be shown in each failed BaseFutureAdapter<FuturePut> above
					FutureDone<List<FutureTask>> initial = Futures.whenAllSuccess(futurePuts).addListener(new BaseFutureAdapter<BaseFuture>() {

						@Override
						public void operationComplete(BaseFuture future) throws Exception {
							if (future.isSuccess()) {
								logger.info("Successfully put and broadcasted all file splits");
							} else {
								logger.info("No success on putting all files splits/broadcasting all keys! Fail reason: " + future.failedReason());
							}
						}
					});
				} else {
					logger.info("No sucess on put(Job): Fail reason: " + future.failedReason());
				}
			}
		});

		// Futures.whenAllSuccess(initial);
	}

}