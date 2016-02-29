package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.storage.DHTWrapper;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Futures;
import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class SimpleBroadcastReceiver implements BroadcastReceiver {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6201919213334638897L;
	private static Logger logger = LoggerFactory.getLogger(SimpleBroadcastReceiver.class);
	// private ThreadPoolExecutor executor;

	public SimpleBroadcastReceiver() {
		// this.executor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>());
	}

	// public SimpleBroadcastReceiver(ThreadPoolExecutor executor) {
	// this.executor = executor;
	// }

	private Job job = null;
	private Number160 peerID;

	@Override
	public void receive(Message message, DHTWrapper dht, ThreadPoolExecutor executor) {
		if (this.peerID == null) {
			this.peerID = dht.peerDHT().peer().peerID();
		}

		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		try {
			if (job == null) {
				FutureGet jobFutureGet = getJobIfNull(input, dht);
				if (jobFutureGet != null) {
					logger.info("Got job");
					Futures.whenAllSuccess(jobFutureGet).addListener(new BaseFutureAdapter<BaseFuture>() {

						@Override
						public void operationComplete(BaseFuture future) throws Exception {
							if (future.isSuccess()) {
								tryExecuteTask(input, dht, executor);
							} else {
								logger.info("No success on job retrieval");
							}
						}

					});
				} else {
					logger.info("Job was null! No job found");
				}
			} else {

				tryExecuteTask(input, dht, executor);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("before super.receive(message)");
	}

	private void tryExecuteTask(NavigableMap<Number640, Data> input, DHTWrapper dht, ThreadPoolExecutor executor) {

		try {
			// This implementation only processes messages from the same peer.
			// Excecption: Initial task (announces the data) and last task (to shutdown the peers)
			Number160 senderId = (Number160) (input.get(NumberUtils.allSameKey("SENDERID")).object());
			Number640 currentTaskId = (Number640) input.get(NumberUtils.allSameKey("CURRENTTASK")).object();
			Number640 initTaskId = (Number640) input.get(NumberUtils.allSameKey("INPUTTASKID")).object(); // All should receive this
			Number640 lastActualTask = (Number640) input.get(NumberUtils.allSameKey("WRITETASKID")).object(); // All should receive this

			if ((job != null && senderId.equals(peerID)) || (currentTaskId.equals(initTaskId)) || currentTaskId.equals(lastActualTask)) {

				Task task = job.findTask((Number640) input.get(NumberUtils.allSameKey("NEXTTASK")).object());
				executor.execute(new Runnable() {

					@Override
					public void run() {
						try {
							task.broadcastReceiver(input, dht);
						} catch (Exception e) { 
							e.printStackTrace();
						}

					}
				});
				if (currentTaskId.equals(lastActualTask)) {
					executor.shutdown();
					int cnt = 0;
					while (!executor.awaitTermination(6, TimeUnit.SECONDS) && cnt++ >= 2) {
						logger.info("Await thread completion");
					}
					executor.shutdownNow();
				}
			} else {
				logger.info("job==null? " + (job == null) + " || !(" + senderId + ").equals(" + peerID + ")?" + (!input.get(NumberUtils.allSameKey("SENDERID")).equals(peerID)) + "||!currentTaskId.equals(initTaskId)?" + (!currentTaskId.equals(initTaskId)) + "|| !currentTaskId.equals(lastActualTask)?"
						+ currentTaskId.equals(lastActualTask));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private FutureGet getJobIfNull(NavigableMap<Number640, Data> dataMap, DHTWrapper dht) throws ClassNotFoundException, IOException {

		Number160 jobKey = (Number160) dataMap.get(NumberUtils.allSameKey("JOBKEY")).object();
		return dht.get(jobKey).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					JobTransferObject serialized = (JobTransferObject) future.data().object();
					job = Job.deserialize(serialized);
					logger.info("Found job " + job);
				} else {
					logger.info("Could not find job");
				}
			}

		});

	}
}
