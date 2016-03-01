package net.tomp2p.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.ThreadPoolExecutor;

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

	private FutureGet jobFutureGet;
	private Job job = null;
	private Number160 peerID;

	@Override
	public void receive(Message message, DHTWrapper dht) {
		if (this.peerID == null) {
			this.peerID = dht.peerDHT().peer().peerID();
		}

		// synchronized (jobFutureGet) {
		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		try {

			if (jobFutureGet == null) {
				jobFutureGet = dht.get(Number160.createHash("JOBKEY")).addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {

							JobTransferObject serialized = (JobTransferObject) future.data().object();
							job = Job.deserialize(serialized);
							logger.info("Found job: " + job);
							tryExecuteTask(input, dht);

							// jobFutureGet.addListener(new BaseFutureAdapter<FutureGet>() {
							//
							// @Override
							// public void operationComplete(FutureGet future) throws Exception {
							// if (future.isCompleted()) {
							// if (job != null) {
							// tryExecuteTask(input, dht);
							// }
							// } else {
							// logger.info("Could not find job");
							// }
							// }
							//
							// });
						} else {
							logger.info("no success on retrieving job. Job = " + job);
						}
					}

				});

			} else {
				if (jobFutureGet.isCompleted()) {
					logger.info("JobFutureGet.isCompleted()? " + jobFutureGet.isCompleted());
					if (job != null) {
						logger.info("Job != null? " + (job != null));
						tryExecuteTask(input, dht);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// }
	}

	private void tryExecuteTask(NavigableMap<Number640, Data> input, DHTWrapper dht) {

		try {
			// This implementation only processes messages from the same peer.
			// Excecption: Initial task (announces the data) and last task (to shutdown the peers)
			Number160 senderId = (Number160) (input.get(NumberUtils.allSameKey("SENDERID")).object());
			Number640 currentTaskId = (Number640) input.get(NumberUtils.allSameKey("CURRENTTASK")).object();
			Number640 initTaskId = (Number640) input.get(NumberUtils.allSameKey("INPUTTASKID")).object(); // All should receive this
			Number640 lastActualTask = (Number640) input.get(NumberUtils.allSameKey("WRITETASKID")).object(); // All should receive this
			// if (currentTaskId.equals(lastActualTask)) {
			Task task = job.findTask((Number640) input.get(NumberUtils.allSameKey("NEXTTASK")).object());
			System.out.println("I " + peerID.intValue() + " received next task to execute from peerid [" + senderId.intValue() + "]: " + task.getClass().getName());
			// }
			if ((job != null && senderId.equals(peerID)) || (currentTaskId.equals(initTaskId)) || currentTaskId.equals(lastActualTask)) {

				// Task task = job.findTask((Number640) input.get(NumberUtils.allSameKey("NEXTTASK")).object());
				task.broadcastReceiver(input, dht);

			} else {
				logger.info(
						"is job not null? " + (job != null) + " || was it a message from myself?" + (input.get(NumberUtils.allSameKey("SENDERID")).equals(peerID)) + "||is it the initial task?" + (currentTaskId.equals(initTaskId)) + "|| Is it the last task?" + currentTaskId.equals(lastActualTask));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// private synchronized FutureGet getJobIfNull(NavigableMap<Number640, Data> dataMap, DHTWrapper dht) throws ClassNotFoundException, IOException {
	//
	// // Number160 jobKey = (Number160) dataMap.get(NumberUtils.allSameKey("JOBKEY")).object();
	// return dht.get(Number160.createHash("JOBKEY")).addListener(new BaseFutureAdapter<FutureGet>() {
	//
	// @Override
	// public void operationComplete(FutureGet future) throws Exception {
	// if (future.isSuccess()) {
	// if (jobFutureGet == null) {
	// JobTransferObject serialized = (JobTransferObject) future.data().object();
	// job = Job.deserialize(serialized);
	// logger.info("Found job " + job);
	// }
	// } else {
	// logger.info("Could not find job");
	// }
	// }
	//
	// });
	//
	// }
}
